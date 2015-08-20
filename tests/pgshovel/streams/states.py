import itertools
import uuid
from collections import namedtuple

import pytest

from pgshovel.interfaces.streams_pb2 import BatchOperation
from pgshovel.replication.validation.transactions import (
    Committed,
    InTransaction,
    InvalidBatch,
    InvalidEventError,
    InvalidPublisher,
    RolledBack,
    StatefulStreamValidator,
    get_operation,
    require_batch_id_advanced_if_same_node,
    require_batch_id_not_advanced_if_same_node,
    require_different_publisher,
    require_same_batch,
    require_same_publisher,
    validate_transaction_state,
)
from tests.pgshovel.streams.fixtures import (
    batch_identifier,
    begin,
    commit,
    copy,
    make_batch_messages,
    message,
    mutation,
)


def test_require_same_batch(message):
    batch_identifier = get_operation(message).batch_identifier

    require_same_batch(
        InTransaction(
            publisher=message.header.publisher,
            batch_identifier=batch_identifier,
        ),
        0,
        message,
    )

    with pytest.raises(InvalidBatch):
        require_same_batch(
            InTransaction(
                publisher=message.header.publisher,
                batch_identifier=copy(batch_identifier, id=batch_identifier.id + 1),
            ),
            0,
            message,
        )


def test_require_batch_advanced_if_same_node(message):
    batch_identifier = get_operation(message).batch_identifier

    require_batch_id_advanced_if_same_node(
        Committed(
            publisher=message.header.publisher,
            batch_identifier=copy(batch_identifier, node=uuid.uuid1().bytes),
        ),
        0,
        message,
    )

    require_batch_id_advanced_if_same_node(
        Committed(
            publisher=message.header.publisher,
            batch_identifier=copy(batch_identifier, id=batch_identifier.id - 1),
        ),
        0,
        message,
    )

    with pytest.raises(InvalidBatch):
        require_batch_id_advanced_if_same_node(
            Committed(
                publisher=message.header.publisher,
                batch_identifier=batch_identifier,
            ),
            0,
            message,
        )


def test_require_batch_id_not_advanced_if_same_node(message):
    batch_identifier = get_operation(message).batch_identifier

    require_batch_id_not_advanced_if_same_node(
        RolledBack(
            publisher=message.header.publisher,
            batch_identifier=batch_identifier,
        ),
        0,
        message,
    )

    require_batch_id_not_advanced_if_same_node(
        RolledBack(
            publisher=message.header.publisher,
            batch_identifier=copy(batch_identifier, node=uuid.uuid1().bytes),
        ),
        0,
        message,
    )

    with pytest.raises(InvalidBatch):
        require_batch_id_not_advanced_if_same_node(
            RolledBack(
                publisher=message.header.publisher,
                batch_identifier=copy(batch_identifier, id=batch_identifier.id + 1),
            ),
            0,
            message,
        )


def test_require_same_publisher(message):
    batch_identifier = get_operation(message).batch_identifier

    require_same_publisher(
        Committed(
            publisher=message.header.publisher,
            batch_identifier=batch_identifier,
        ),
        0,
        message,
    )

    with pytest.raises(InvalidPublisher):
        require_same_publisher(
            Committed(
                publisher=uuid.uuid1().bytes,
                batch_identifier=batch_identifier,
            ),
            0,
            message,
        )


def test_require_different_publisher(message):
    batch_identifier = get_operation(message).batch_identifier

    require_different_publisher(
        Committed(
            publisher=uuid.uuid1().bytes,  # change the publisher
            batch_identifier=batch_identifier,
        ),
        0,
        message,
    )

    with pytest.raises(InvalidPublisher):
        require_different_publisher(
            Committed(
                publisher=message.header.publisher,
                batch_identifier=batch_identifier,
            ),
            0,
            message,
        )


def test_stateful_validator():
    Locked = namedtuple('Locked', '')
    Unlocked = namedtuple('Unlocked', '')

    validator = StatefulStreamValidator({
        Unlocked: {
            'coin': lambda state, event: Unlocked(),
            'push': lambda state, event: Locked(),
        },
        Locked: {
            'coin': lambda state, event: Unlocked(),
            'push': lambda state, event: Locked(),
        },
    }, start=Locked())

    assertions = (
        ('push', Locked()),
        ('coin', Unlocked()),
        ('coin', Unlocked()),
        ('push', Locked()),
        ('push', Locked()),
    )

    inputs = (i[0] for i in assertions)
    validated = validator(i[0] for i in assertions)
    expected = (i[1] for i in assertions)
    for input, (state, event), expected in itertools.izip(inputs, validated, expected):
        assert input == event
        assert state == expected

    with pytest.raises(InvalidEventError):
        next(validator(('kick',)))


def test_stateful_validator_unhandled_starting_state():
    events = range(5)
    validator = StatefulStreamValidator({})
    validated = validator(events)

    with pytest.raises(InvalidEventError):
        next(validated)


def test_successful_transaction():
    messages = list(make_batch_messages(batch_identifier, [
        {'begin_operation': begin},
        {'mutation_operation': mutation},
        {'commit_operation': commit},
    ]))

    validated = validate_transaction_state(messages)

    assert next(validated) == (
        InTransaction(
            publisher=messages[0].header.publisher,
            batch_identifier=batch_identifier
        ),
        messages[0]
    )
    assert next(validated) == (
        InTransaction(
            publisher=messages[1].header.publisher,
            batch_identifier=batch_identifier
        ),
        messages[1]
    )
    assert next(validated) == (
        Committed(
            publisher=messages[2].header.publisher,
            batch_identifier=batch_identifier
        ),
        messages[2]
    )


# TODO: Add test to ensure that {Committed,RolledBack} can transition to
# InTransaction after a publisher change.
