"""Kafka transport contract for the six Processed topics.

The Kafka Key partitions by result dimension. A deterministic identity derived
from the window detects a replay; it does not make the Kafka sink exactly-once.
"""
import hashlib
import json
import re
from datetime import datetime


TOPIC_CONTRACTS = {
    'TimeMetricsProcessedCoin': {
        'schema': 'time_metrics_schema', 'key_fields': ('region', 'coin_symbol')},
    'ArbitrageProcessedCoin': {
        'schema': 'arbitrage_schema', 'key_fields': ('coin_symbol',)},
    'OrderbookProcessedRegionCoin': {
        'schema': 'orderbook_cal_schema', 'key_fields': ('region', 'symbol')},
    'OrderbookProcessedAllRegionCoin': {
        'schema': 'orderbook_cal_all_schema', 'key_fields': ('symbol',)},
    'MarketArbitrageProcessedCoin': {
        'schema': 'market_arbitrage_schema', 'key_fields': ('coin_symbol',)},
    'MarketArbitrageSignalProcessedCoin': {
        'schema': 'market_arbitrage_signal_schema', 'key_fields': ('market', 'coin_symbol')},
}

DIMENSION = re.compile(r'[A-Z0-9_]+\Z')


def _window_time(value, field):
    if not isinstance(value, str) or not value.endswith('Z'):
        raise ValueError(f'{field} must be a UTC ISO-8601 string')
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as error:
        raise ValueError(f'{field} must be a UTC ISO-8601 string') from error


def validate_record(topic, row):
    """Require fields needed by the partition key and replay identity."""
    try:
        fields = TOPIC_CONTRACTS[topic]['key_fields']
    except KeyError as error:
        raise ValueError(f'unknown Processed topic: {topic}') from error
    for field in fields:
        value = row.get(field)
        if not isinstance(value, str) or not DIMENSION.fullmatch(value):
            raise ValueError(f'{topic}.{field} must be uppercase and non-empty')
    start = _window_time(row.get('start'), 'start')
    end = _window_time(row.get('end'), 'end')
    if end <= start:
        raise ValueError(f'{topic}.end must be later than start')


def key_for(topic, row):
    validate_record(topic, row)
    return ':'.join(row[field] for field in TOPIC_CONTRACTS[topic]['key_fields']).encode('utf-8')


def identity_for(topic, key, row):
    validate_record(topic, row)
    if not isinstance(key, bytes) or key != key_for(topic, row):
        raise ValueError(f'{topic} Kafka Key does not match Value dimensions')
    start = _window_time(row['start'], 'start').isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    end = _window_time(row['end'], 'end').isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    canonical = json.dumps(
        [topic, key.decode('utf-8'), start, end],
        ensure_ascii=False, separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(canonical).hexdigest()
