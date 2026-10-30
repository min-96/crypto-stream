"""Kafka boundary for externally converted KRW ticker and orderbook records."""

import json
import logging
import math
import re
from datetime import datetime


REGIONS = {'korea': 'Korea', 'ne': 'NE', 'asia': 'Asia'}
KINDS = {'ticker': 'Ticker', 'orderbook': 'Orderbook'}
TICKER_FIELDS = (
    'opening_price', 'trade_price', 'max_price', 'min_price',
    'prev_closing_price', 'acc_trade_volume_24h',
    'signed_change_price', 'signed_change_rate',
)
ORDERBOOK_FIELDS = (
    'region', 'market', 'symbol', 'highest_bid', 'lowest_ask',
    'total_bid_volume', 'total_ask_volume', 'timestamp',
)
MAX_ORDERBOOK_ITEMS = 1000
MAX_OUTPUT_BYTES = 900_000
KEY_PATTERN = re.compile(rb'[A-Z0-9]+:[A-Z0-9]+:[A-Z0-9]+\Z')
TICKER_PRICE_FIELDS = ('opening_price', 'trade_price', 'max_price', 'min_price', 'prev_closing_price')
DECIMAL_PATTERN = re.compile(r'[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?\Z')

logger = logging.getLogger(__name__)


class InvalidTickerValue(ValueError):
    """A well-formed Ticker whose value is unusable; skip it instead of stopping (ADR-019)."""


def route(topic):
    match = re.fullmatch(r'(korea|ne|asia)SocketDataInBTC-(ticker|orderbook)', topic)
    if not match:
        raise ValueError(f'unsupported Raw topic: {topic}')
    region, kind = match.groups()
    return region, kind, f'Region{REGIONS[region]}_{KINDS[kind]}Preprocessing'


def output_topic(topic):
    return route(topic)[2]


def _finite_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _check_ticker_values(data):
    for name in TICKER_FIELDS:
        value = data[name]
        if value is None:
            raise InvalidTickerValue(f'{name} is null')
        if not DECIMAL_PATTERN.match(value):
            raise InvalidTickerValue(f'{name} is not a decimal number: {value!r}')
        number = float(value)
        if not math.isfinite(number):
            raise InvalidTickerValue(f'{name} is out of double range: {value!r}')
        if name in TICKER_PRICE_FIELDS and number <= 0:
            raise InvalidTickerValue(f'{name} must be positive: {value!r}')


def validate(record, region, kind, key):
    exchange, coin, _quote = key.decode('ascii').split(':')
    if kind == 'ticker':
        if not isinstance(record, dict) or set(record) != {'market', 'timestamp', 'coin_symbol', 'data'}:
            raise ValueError('ticker must match market_schema')
        if record['market'] != exchange or record['coin_symbol'] != coin or not _finite_number(record['timestamp']):
            raise ValueError('ticker identity or timestamp mismatch')
        data = record['data']
        if not isinstance(data, dict) or set(data) != set(TICKER_FIELDS):
            raise ValueError('ticker data must match ticker_data_schema')
        if any(data[name] is not None and not isinstance(data[name], str) for name in TICKER_FIELDS):
            raise ValueError('ticker data fields must be strings or null')
        _check_ticker_values(data)
    else:
        if not isinstance(record, list) or not 1 <= len(record) <= MAX_ORDERBOOK_ITEMS:
            raise ValueError('orderbook array length out of range')
        for item in record:
            if not isinstance(item, dict) or set(item) != set(ORDERBOOK_FIELDS):
                raise ValueError('orderbook item must match orderbook_data_schema')
            if (item['region'] != REGIONS[region] or item['market'] != exchange
                    or item['symbol'] != coin):
                raise ValueError('orderbook identity mismatch')
            if any(not _finite_number(item[name]) for name in (
                    'highest_bid', 'lowest_ask', 'total_bid_volume', 'total_ask_volume')):
                raise ValueError('orderbook price or volume must be finite numbers')
            try:
                datetime.fromisoformat(item['timestamp'].replace('Z', '+00:00'))
            except (AttributeError, TypeError, ValueError) as error:
                raise ValueError('orderbook timestamp must be ISO 8601') from error


async def process_record(raw, producer, convert):
    region, kind, topic = route(raw.topic)
    key = raw.key
    if not isinstance(key, bytes) or not KEY_PATTERN.fullmatch(key):
        raise ValueError('Raw Kafka key must be EXCHANGE:COIN:QUOTE')
    if not isinstance(raw.value, bytes):
        raise ValueError('Raw Kafka value must be bytes')
    try:
        converted = convert(raw.value, key)
        validate(converted, region, kind, key)
    except InvalidTickerValue as error:
        exchange, coin, _quote = key.decode('ascii').split(':')
        logger.warning('skipped invalid ticker: exchange=%s coin=%s topic=%s partition=%s offset=%s reason=%s',
                       exchange, coin, raw.topic, raw.partition, raw.offset, error)
        return None
    value = json.dumps(converted, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
    if len(value) > MAX_OUTPUT_BYTES:
        raise ValueError('Preprocessed record exceeds output size limit')
    return await producer.send_and_wait(topic, key=key, value=value)
