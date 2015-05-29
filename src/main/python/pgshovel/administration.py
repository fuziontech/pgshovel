import cPickle as pickle
import collections
import hashlib
import itertools
import logging
import operator
import random

import psycopg2
from pkg_resources import parse_version

from pgshovel import __version__
from pgshovel.cluster import check_version
from pgshovel.database import (
    get_configuration_value,
    get_node_identifier,
    get_or_set_node_identifier,
    set_configuration_value,
    update_configuration_value,
)
from pgshovel.interfaces.configurations_pb2 import (
    ClusterConfiguration,
    ReplicationSetConfiguration,
)
from pgshovel.utilities import unique
from pgshovel.utilities.datastructures import FormattedSequence
from pgshovel.utilities.postgresql import (
    Transaction,
    managed,
    quote,
)
from pgshovel.utilities.protobuf import BinaryCodec
from pgshovel.utilities.templates import resource_string
from pgshovel.utilities.zookeeper import commit


logger = logging.getLogger(__name__)


# Database Management

# The configuration table is used to store host-specific metadata, such as a
# unique identifier that is intended to correspond to this (and only this)
# database instance. This can be contrasted with the data that is stored in
# ZooKeeper, which is intended for configuration data that can be shared among
# all nodes in the cluster. (As such, this table should **not** be replicated
# to other nodes in the cluster if using asynchronous replication.)

INSTALL_CONFIGURATION_TABLE_STATEMENT_TEMPLATE = """\
CREATE TABLE IF NOT EXISTS {schema}.configuration (
    key varchar PRIMARY KEY,
    value bytea
)
"""

def create_configuration_table(cluster, cursor):
    statement = INSTALL_CONFIGURATION_TABLE_STATEMENT_TEMPLATE.format(
        schema=quote(cluster.schema),
    )
    cursor.execute(statement)


INSTALL_LOG_TRIGGER_STATEMENT_TEMPLATE = """\
CREATE OR REPLACE FUNCTION {schema}.log()
RETURNS trigger
LANGUAGE plpythonu AS
$TRIGGER$
# Generated by pgshovel=={version}
{body}
$TRIGGER$"""


def create_log_trigger_function(cluster, cursor, node_id):
    """
    Installs the log trigger function on the database for the provided cursor,
    returning the function name that can be used as part of a ``CREATE
    TRIGGER`` statement.
    """
    body = resource_string('sql/log_trigger.py.tmpl')
    statement = INSTALL_LOG_TRIGGER_STATEMENT_TEMPLATE.format(
        schema=quote(cluster.schema),
        body=body,
        version=__version__,
    )
    cursor.execute(statement)


def setup_database(cluster, cursor):
    """
    Configures a database (the provided cursor) for use with pgshovel.

    This function can also be used to repair a broken installation, or update
    an existing installation's log trigger function.
    """
    # Install PGQ if it doesn't already exist.
    logger.info('Creating PgQ extension (if it does not already exist)...')
    cursor.execute('CREATE EXTENSION IF NOT EXISTS pgq')

    # Install pypythonu if it doesn't already exist.
    logger.info('Creating (or updating) plpythonu language...')
    cursor.execute('CREATE OR REPLACE LANGUAGE plpythonu')

    # Create the schema if it doesn't already exist.
    logger.info('Creating schema (if it does not already exist)...')
    cursor.execute('CREATE SCHEMA IF NOT EXISTS {schema}'.format(
        schema=quote(cluster.schema),
    ))

    # Create the configuration table if it doesn't already exist.
    logger.info('Creating configuration table (if it does not already exist)...')
    create_configuration_table(cluster, cursor)

    version = get_configuration_value(cluster, cursor, 'version')
    if version is None:
        set_configuration_value(cluster, cursor, 'version', __version__)
    elif version is not None and str(version) != __version__:
        update_configuration_value(cluster, cursor, 'version', __version__)

    # Ensure that this database has already had an identifier associated with it.
    logger.info('Checking for node ID...')
    node_id = get_or_set_node_identifier(cluster, cursor)

    logger.info('Installing (or updating) log trigger function...')
    create_log_trigger_function(cluster, cursor, node_id)

    return node_id


def get_managed_databases(cluster, dsns, configure=True, skip_inaccessible=False, same_version=True):
    """
    Returns a dictionary of managed databases by their unique node ID. If the
    same node is referenced multiple times (either by the same, or by different
    DSNs), an error is raised.

    If the database has not already been configured for use with pgshovel, the
    database will be implicitly configured, unless the ``configure`` argument
    is ``False``, in which case it will error. If the same node is attempted to
    be configured multiple times (by providing the same DSN multiple times, or
    diffrent DSNs that point to the same database) an error is raised to
    prevent deadlocking during configuration.

    By default, all databases must be accessible. If partial results are
    acceptable (such as cases where databases may be expected to have
    permanently failed), the ``skip_inaccessible`` arguments allows returning
    only those databases that are able to be connected to and an error is
    logged.
    """
    if not dsns:
        return {}

    nodes = {}

    if same_version:
        ztransaction = check_version(cluster)
    else:
        ztransaction = cluster.zookeeper.transaction()

    lock_id = random.randint(-2**63, 2**63-1)  # bigint max/min
    logger.debug('Connecting to databases: %s', FormattedSequence(dsns))

    transactions = []

    for dsn in dsns:
        try:
            connection = psycopg2.connect(dsn)
        except Exception as error:
            if skip_inaccessible:
                logger.warning('%s is inaccessible due to error, skipping: %s', dsn, error)
                continue
            else:
                raise

        logger.debug('Checking if %s has been configured...', dsn)
        try:
            with connection.cursor() as cursor:
                node_id = get_node_identifier(cluster, cursor)
                assert node_id is not None
        except psycopg2.ProgrammingError:
            if not configure:
                raise

            # TODO: Check this better to ensure this is the right type of error
            # (make sure that is specific enough to the table not being
            # present.)
            logger.info('%s has not been configured for use, setting up now...', dsn)
            connection.rollback()  # start over

            transaction = Transaction(connection, 'setup-database')
            transactions.append(transaction)
            with connection.cursor() as cursor:
                # To ensure that we're not attempting to configure the same
                # database multiple times (which would result in a deadlock,
                # since the second transaction will block indefinitely, waiting
                # for the first transaction to be committed or rolled back) we
                # take out an advisory lock to check that we haven't already
                # prepared this database. (We can't simply check for the
                # existence of the configuration table at this point, since
                # that transaction has not been committed yet.)
                cursor.execute('SELECT pg_try_advisory_lock(%s) as acquired', (lock_id,))
                ((acquired,),) = cursor.fetchall()
                assert acquired, 'could not take out advisory lock on %s (possible deadlock?)' % (connection,)

                node_id = setup_database(cluster, cursor)
        else:
            # Check to ensure that the remote database is configured using the
            # same version as the local version. This is important since a
            # previously configured database that has not been used for some
            # time can still have an old version of the schema, log trigger,
            # etc. Adding it back to the cluster without upgrading it can cause
            # strange compatibility issues.
            # TODO: It would make sense here to provide an easy upgrade path --
            # right now, there is no direct path to upgrading a database that
            # has no groups associated with it!
            with connection.cursor() as cursor:
                version = str(get_configuration_value(cluster, cursor, 'version'))
                assert version == __version__, 'local and node versions do not match (local: %s, node: %s)' % (__version__, version)

            logger.debug('%s is already configured as %s (version %s).', dsn, node_id, version)
            connection.commit()  # don't leave idle in transaction

        assert node_id not in nodes, 'found duplicate node: %s and %s' % (connection, nodes[node_id])
        nodes[node_id] = connection

    if transactions:
        with managed(transactions):
            commit(ztransaction)

    return nodes


# Trigger Management


def setup_triggers(cluster, cursor, name, configuration):
    trigger = cluster.get_trigger_name(name)

    def create_trigger(schema, table, primary_keys, columns):
        logger.info('Installing (or replacing) log trigger on %s.%s...', schema, table)

        statement = """
            CREATE TRIGGER {name}
            AFTER INSERT OR UPDATE OF {columns} OR DELETE
            ON {schema}.{table}
            FOR EACH ROW EXECUTE PROCEDURE {cluster_schema}.log(%s, %s, %s, %s)
        """.format(
            name=quote(trigger),
            columns=', '.join(map(quote, columns)),
            table=quote(table),
            schema=quote(schema),
            cluster_schema=quote(cluster.schema),
        )

        cursor.execute("DROP TRIGGER IF EXISTS {name} ON {schema}.{table}".format(
            name=quote(trigger),
            schema=quote(schema),
            table=quote(table),
        ))

        cursor.execute(statement, (
            cluster.get_queue_name(name),
            pickle.dumps(primary_keys),
            pickle.dumps(columns),
            get_version(configuration)),
        )

    for table in configuration.tables:
        primary_keys = unique(list(table.primary_keys))
        create_trigger(
            table.schema,
            table.name,
            primary_keys,
            unique(primary_keys + list(table.columns)),
        )


def drop_trigger(cluster, cursor, name, schema, table):
    """
    Drops a log trigger on the provided table for the specified replication set.
    """
    logger.info('Dropping log trigger on %s.%s...', schema, table)
    cursor.execute('DROP TRIGGER {name} ON {schema}.{table}'.format(
        name=quote(cluster.get_trigger_name(name)),
        schema=quote(schema),
        table=quote(table),
    ))


# Replication Set Management

def get_version(configuration):
    """
    Returns a MD5 hash (version identifier) for a configuration object.
    """
    return hashlib.md5(configuration.SerializeToString()).hexdigest()


class VersionedSet(collections.namedtuple('VersionedSet', 'name version')):
    @classmethod
    def expand(cls, value):
        bits = value.split('@', 1)
        if len(bits) == 1:
            return cls(bits[0], None)
        elif len(bits) == 2:
            return cls(bits[0], bits[1])
        else:
            raise AssertionError('Invalid set identifier: %r' % (value,))


def fetch_sets(cluster, names=None):
    if names is None:
        names = cluster.zookeeper.get_children(cluster.get_set_path())

    sets = map(VersionedSet.expand, names)
    paths = map(
        cluster.get_set_path,
        map(operator.attrgetter('name'), sets),
    )
    futures = map(cluster.zookeeper.get_async, paths)

    results = []
    decode = BinaryCodec(ReplicationSetConfiguration).decode
    for s, future in zip(sets, futures):
        data, stat = future.get()
        configuration = decode(data)
        assert s.version is None or s.version == get_version(configuration), \
            'versions do not match (%s and %s)' % (s.version, get_version(configuration))
        results.append((s.name, (configuration, stat)))

    return results


def validate_set_configuration(configuration):
    assert len(configuration.databases) > 0, 'set must have associated databases'

    # TODO: It might make sense to normalize the database dsns here just to be
    # doubly safe.
    dsns = set()
    for database in configuration.databases:
        assert database.dsn not in dsns, 'duplicate databases not allowed: %s' % (database.dsn,)
        dsns.add(database.dsn)

    for table in configuration.tables:
        assert len(table.primary_keys) > 0, 'table %s.%s must have associated primary key column(s)' % (quote(table.schema), quote(table.name),)


def configure_set(cluster, cursor, name, configuration, previous_configuration=None):
    """
    Configures a replication set using the provided name and configuration data.
    """
    logger.info('Configuring replication set on %s...', cursor.connection.dsn)

    # Create the transaction queue if it doesn't already exist.
    logger.info('Creating transaction queue (if it does not already exist)...')
    cursor.execute("SELECT pgq.create_queue(%s)", (cluster.get_queue_name(name),))

    setup_triggers(cluster, cursor, name, configuration)

    if previous_configuration is not None:
        current_tables = set((t.schema, t.name) for t in previous_configuration.tables)
        updated_tables = set((t.schema, t.name) for t in configuration.tables)
        dropped_tables = current_tables - updated_tables

        for schema, table in dropped_tables:
            drop_trigger(cluster, cursor, name, schema, table)


def unconfigure_set(cluster, cursor, name, configuration):
    """
    Removes all triggers and log queue for the provided replication set.
    """
    logger.info('Unconfiguring replication set on %s...', cursor.connection.dsn)

    # Drop the transaction queue if it exists.
    logger.info('Dropping transaction queue...')
    cursor.execute("SELECT pgq.drop_queue(%s)", (cluster.get_queue_name(name),))

    for table in configuration.tables:
        drop_trigger(cluster, cursor, name, table.schema, table.name)


# Cluster Management

def initialize_cluster(cluster):
    """
    Initialize a pgshovel cluster in ZooKeeper.
    """
    logger.info('Creating a new cluster for %s...', cluster)

    configuration = ClusterConfiguration(version=__version__)
    ztransaction = cluster.zookeeper.transaction()
    ztransaction.create(cluster.path, BinaryCodec(ClusterConfiguration).encode(configuration))
    ztransaction.create(cluster.get_set_path())
    commit(ztransaction)


def upgrade_cluster(cluster, force=False):
    zookeeper = cluster.zookeeper

    codec = BinaryCodec(ClusterConfiguration)
    data, stat = zookeeper.get(cluster.path)
    configuration = codec.decode(data)

    # if the configuration is newer or equal, require manual intervention
    assert parse_version(__version__) > parse_version(configuration.version) or force, 'cannot downgrade %s to %s' % (configuration.version, __version__)

    logger.info('Upgrading cluster from %s to %s...', configuration.version, __version__)
    configuration.version = __version__

    ztransaction = zookeeper.transaction()
    ztransaction.set_data(cluster.path, codec.encode(configuration), version=stat.version)

    # collect databases
    databases = set()
    for s, (configuration, stat) in fetch_sets(cluster):
        for database in configuration.databases:
            databases.add(database.dsn)

        # TODO: not entirely sure that this is necessary, but can't hurt
        ztransaction.check(cluster.get_set_path(s), version=stat.version)

    transactions = []
    # get_managed_databases prevents duplicates, so this is safe to perform
    # without doing any advisory locking (although it will error if two sets
    # refer to the same database using different DSNs.) get_managed_databases
    # should provide some capacity for doing deduplication to make this more
    # convenient, probably, but this at least keeps it from inadvertently
    # breaking for now.
    for connection in get_managed_databases(cluster, databases, configure=False, same_version=False).values():
        transaction = Transaction(connection, 'update-cluster')
        transactions.append(transaction)
        with connection.cursor() as cursor:
            setup_database(cluster, cursor)

    with managed(transactions):
        commit(ztransaction)


# Replication Set Management

def create_set(cluster, name, configuration):
    # TODO: add dry run support

    validate_set_configuration(configuration)

    databases = get_managed_databases(cluster, [d.dsn for d in configuration.databases])

    ztransaction = check_version(cluster)

    transactions = []
    for connection in databases.values():
        transaction = Transaction(connection, 'create-set:%s' % (name,))
        transactions.append(transaction)

        with connection.cursor() as cursor:
            configure_set(cluster, cursor, name, configuration)

    ztransaction.create(
        cluster.get_set_path(name),
        BinaryCodec(ReplicationSetConfiguration).encode(configuration),
    )

    with managed(transactions):
        commit(ztransaction)


def update_set(cluster, name, updated_configuration, allow_forced_removal=False):
    # TODO: add dry run support

    validate_set_configuration(updated_configuration)

    (name, (current_configuration, stat)) = fetch_sets(cluster, (name,))[0]

    # TODO: It probably makes sense to normalize the database URIs here.
    current_databases = set(d.dsn for d in current_configuration.databases)
    updated_databases = set(d.dsn for d in updated_configuration.databases)

    additions = get_managed_databases(cluster, updated_databases - current_databases)
    mutations = get_managed_databases(cluster, updated_databases & current_databases)

    deletions = get_managed_databases(
        cluster,
        current_databases - updated_databases,
        skip_inaccessible=allow_forced_removal,
    )

    # ensure no items show up multiple times, since that causes incorrect behavior
    # TODO: this is a very naive approach to avoid shooting ourselves in the
    # foot and could be improved for valid cases (updating a dsn for an
    # existing set should be treated as a mutation, not an addition and
    # deletion) but this would require a more intelligent implementation
    occurrences = collections.Counter()
    for nodes in map(operator.methodcaller('keys'), (additions, mutations, deletions)):
        occurrences.update(nodes)

    duplicates = list(itertools.takewhile(lambda (node, count): count > 1, occurrences.most_common()))
    assert not duplicates, 'found duplicates: %s' % (duplicates,)

    ztransaction = check_version(cluster)

    transactions = []

    for connection in additions.values():
        transaction = Transaction(connection, 'update-set:create:%s' % (name,))
        transactions.append(transaction)
        with connection.cursor() as cursor:
            configure_set(cluster, cursor, name, updated_configuration, None)

    for connection in mutations.values():
        transaction = Transaction(connection, 'update-set:update:%s' % (name,))
        transactions.append(transaction)
        with connection.cursor() as cursor:
            configure_set(cluster, cursor, name, updated_configuration, current_configuration)

    # TODO: add help to inform user of the possiblity of retry
    for connection in deletions.values():
        transaction = Transaction(connection, 'update-set:delete:%s' % (name,))
        transactions.append(transaction)
        with connection.cursor() as cursor:
            unconfigure_set(cluster, cursor, name, current_configuration)

    ztransaction.set_data(
        cluster.get_set_path(name),
        BinaryCodec(ReplicationSetConfiguration).encode(updated_configuration),
        version=stat.version,
    )

    with managed(transactions):
        commit(ztransaction)


def drop_set(cluster, name, allow_forced_removal=False):
    # TODO: add dry run support

    (name, (configuration, stat)) = fetch_sets(cluster, (name,))[0]

    deletions = get_managed_databases(
        cluster,
        [d.dsn for d in configuration.databases],
        configure=False,
        skip_inaccessible=allow_forced_removal,
    )

    ztransaction = check_version(cluster)

    transactions = []

    # TODO: add help to inform user of the possiblity of retry
    for connection in deletions.values():
        transaction = Transaction(connection, 'drop-set:%s' % (name,))
        transactions.append(transaction)
        with connection.cursor() as cursor:
            unconfigure_set(cluster, cursor, name, configuration)

    ztransaction.delete(
        cluster.get_set_path(name),
        version=stat.version,
    )

    with managed(transactions):
        commit(ztransaction)
