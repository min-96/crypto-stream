"""Commit only the Raw record whose Preprocessed write was acknowledged."""

from aiokafka import TopicPartition


async def publish_then_commit(raw, publish, consumer, *, after_ack=None):
    await publish(raw)
    if after_ack is not None:
        after_ack(raw)
    await consumer.commit({TopicPartition(raw.topic, raw.partition): raw.offset + 1})


async def publish_then_commit_transactionally(raw, publish, producer, group_id, *,
                                               after_ack=None):
    """Atomically expose output and the consumed Raw offset to Kafka readers."""
    if not group_id:
        raise ValueError('transactional preprocessing requires a consumer group')
    async with producer.transaction():
        await publish(raw)
        if after_ack is not None:
            after_ack(raw)
        await producer.send_offsets_to_transaction(
            {TopicPartition(raw.topic, raw.partition): raw.offset + 1}, group_id)


async def publish_batch_transactionally(records, publish, producer, group_id, *,
                                        after_ack=None):
    """Expose a fetched batch and its final per-partition offsets atomically.

    Records for each partition must retain fetch order. Kafka offsets may have
    gaps (for example, aborted transactions), so only strictly increasing
    offsets are required.
    """
    if not group_id:
        raise ValueError('transactional preprocessing requires a consumer group')
    if not records:
        raise ValueError('transactional batch must not be empty')
    offsets = {}
    for raw in records:
        tp = TopicPartition(raw.topic, raw.partition)
        if tp in offsets and raw.offset < offsets[tp]:
            raise ValueError(f'Raw offsets out of order: {tp}')
        offsets[tp] = raw.offset + 1
    async with producer.transaction():
        for raw in records:
            await publish(raw)
            if after_ack is not None:
                after_ack(raw)
        await producer.send_offsets_to_transaction(offsets, group_id)
