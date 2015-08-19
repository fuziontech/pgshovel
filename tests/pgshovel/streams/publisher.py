import pytest

from pgshovel.replication.validation import consumers
from pgshovel.replication.validation import transactions
from pgshovel.streams.publisher import Publisher
from pgshovel.utilities.protobuf import get_oneof_value
from tests.pgshovel.streams.fixtures import (
    batch_identifier,
    begin,
    commit,
    mutation,
    reserialize,
    rollback,
)


def test_publisher():
    messages = []
    publisher = Publisher(messages.extend)

    with publisher.batch(batch_identifier, begin) as publish:
        publish(mutation)

    published_messages = map(reserialize, messages)

    assert get_oneof_value(
        get_oneof_value(published_messages[0], 'operation'),
        'operation'
    ) == begin
    assert get_oneof_value(
        get_oneof_value(published_messages[1], 'operation'),
        'operation'
    ) == mutation
    assert get_oneof_value(
        get_oneof_value(published_messages[2], 'operation'),
        'operation'
    ) == commit

    for i, message in enumerate(published_messages):
        assert message.header.publisher == publisher.id
        assert message.header.sequence == i

    # Ensure it actually generates valid data.
    assert list(transactions.validate_transaction_state(published_messages))
    assert list(consumers.validate_consumer_state(published_messages))


def test_publisher_failure():
    messages = []
    publisher = Publisher(messages.extend)

    with pytest.raises(NotImplementedError):
        with publisher.batch(batch_identifier, begin):
            raise NotImplementedError

    published_messages = map(reserialize, messages)

    assert get_oneof_value(
        get_oneof_value(published_messages[0], 'operation'),
        'operation'
    ) == begin
    assert get_oneof_value(
        get_oneof_value(published_messages[1], 'operation'),
        'operation'
    ) == rollback

    # Ensure it actually generates valid data.
    assert list(transactions.validate_transaction_state(published_messages))
    assert list(consumers.validate_consumer_state(published_messages))

    for i, message in enumerate(published_messages):
        assert message.header.publisher == publisher.id
        assert message.header.sequence == i

    # Write another message to ensure that the publisher can continue to be used.
    assert len(messages) == 2
    publisher.publish()
    assert len(messages) == 3
    assert messages[2].header.sequence == 2
