"""
Tools for aiding batch consumption.
"""
import itertools

from pgshovel.interfaces.streams_pb2 import (
    BatchOperation,
    BeginOperation,
    CommitOperation,
    MutationOperation,
    RollbackOperation,
)


def get_operation(message):
    if message is None:
        return None

    return getattr(message, message.WhichOneof('operation'))


class TransactionFailed(Exception):
    """
    Exception raised when a transaction is cancelled or failed for any reason.
    """


class TransactionAborted(TransactionFailed):
    """
    Exception raised when a transaction is rolled back implicitly due the end
    of a transaction stream without a terminal Commit or Rollback command.

    This can happen if a relay process crashes or is partitioned from the
    network in the middle of publishing a transaction, and restarts publishing
    the batch from the beginning when it recovers.
    """


class TransactionCancelled(TransactionFailed):
    """
    Exception raised when a transaction is explicitly cancelled via a Rollback
    operation.

    This can happen if a relay process detects an error while publishing the
    transaction -- for example, if the relay process has it's connection
    severed to the database, but can still publish to Kafka to inform consumers
    that it will be restarting the publication of the batch.
    """


def batched(messages):
    """
    Creates an iterator out of a stream of messages that have already undergone
    validation. The iterator yields a ``(batch, mutations)`` tuple, where the
    ``mutations`` member is an iterator of ``Mutation`` objects.

    If the transaction is aborted for any reason (either due to an unexpected
    end of transaction, or an explicit rollback), the operation iterator will
    raise a ``TransactionAborted`` exception, in which the transaction should
    also be rolled back on the destination. If the mutation iterator completes
    without an error, the transaction was retrieved in it's entirety from the
    stream and can be committed on the destination, and then marked as
    completed in the transaction log.
    """
    def make_mutation_iterator(messages):
        for message in messages:
            # Only batch operations are supported in this context.
            operation = get_operation(message)
            assert isinstance(operation, BatchOperation)

            operation = get_operation(operation)

            if isinstance(operation, BeginOperation):
                continue  # skip
            elif isinstance(operation, MutationOperation):
                yield operation
            elif isinstance(operation, CommitOperation):
                return
            elif isinstance(operation, RollbackOperation):
                raise TransactionCancelled('Transaction rolled back.')
            else:
                raise ValueError('Unexpected operation in transaction.')

        raise TransactionAborted('Unexpected end of transaction iterator.')

    key = lambda (state, message): (message.header.publisher, state.batch_identifier)
    for (publisher, batch_identifier), items in itertools.groupby(messages, key):
        yield batch_identifier, make_mutation_iterator(i[1] for i in items)
