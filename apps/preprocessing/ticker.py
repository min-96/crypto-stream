"""Dispatch KRW ticker frames by the original Kafka key's exchange."""

from bithumb import convert_ticker as convert_bithumb_ticker
from upbit import convert_ticker as convert_upbit_ticker


CONVERTERS = {
    'UPBIT': convert_upbit_ticker,
    'BITHUMB': convert_bithumb_ticker,
}


def convert_ticker(raw, key):
    if not isinstance(key, bytes):
        raise ValueError('Ticker Kafka key must be bytes')
    try:
        exchange = key.decode('ascii').split(':')[0]
    except UnicodeDecodeError as error:
        raise ValueError('invalid Ticker Kafka key') from error
    try:
        converter = CONVERTERS[exchange]
    except KeyError as error:
        raise ValueError(f'unsupported Ticker exchange: {exchange}') from error
    return converter(raw, key)
