"""Normalize Bithumb KRW WebSocket ticker frames to market_schema."""

import json
import math
from datetime import datetime, timezone


def _frame(raw, key):
    if not isinstance(raw, bytes) or not isinstance(key, bytes):
        raise ValueError('Bithumb frame and Kafka key must be bytes')
    try:
        frame = json.loads(raw)
        exchange, coin, quote = key.decode('ascii').split(':')
        frame_quote, frame_coin = frame['code'].split('-')
    except (UnicodeDecodeError, ValueError, KeyError, TypeError, AttributeError) as error:
        raise ValueError('invalid Bithumb frame or Kafka key') from error
    if (not isinstance(frame, dict) or frame.get('type') != 'ticker'
            or exchange != 'BITHUMB' or quote != 'KRW'
            or frame_quote != quote or frame_coin != coin):
        raise ValueError('Bithumb frame type or trading pair does not match Kafka key')
    return frame, coin


def _ticker_value(frame, name):
    """Leave value errors to the common Ticker checks (ADR-019)."""
    if name not in frame:
        raise ValueError(f'missing Bithumb {name}')
    value = frame[name]
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f'Bithumb {name} must be a JSON number or null')
    return str(value)


def _timestamp(frame):
    value = frame.get('timestamp')
    if (not isinstance(value, (int, float)) or isinstance(value, bool)
            or not math.isfinite(value) or value <= 0):
        raise ValueError('invalid Bithumb timestamp')
    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).timestamp()
    except (OverflowError, OSError, ValueError) as error:
        raise ValueError('invalid Bithumb timestamp') from error


def convert_ticker(raw, key):
    frame, coin = _frame(raw, key)
    fields = {
        'opening_price': 'opening_price',
        'trade_price': 'trade_price',
        'max_price': 'high_price',
        'min_price': 'low_price',
        'prev_closing_price': 'prev_closing_price',
        'acc_trade_volume_24h': 'acc_trade_volume_24h',
        'signed_change_price': 'signed_change_price',
        'signed_change_rate': 'signed_change_rate',
    }
    data = {target: _ticker_value(frame, source) for target, source in fields.items()}
    return {'market': 'BITHUMB', 'timestamp': _timestamp(frame),
            'coin_symbol': coin, 'data': data}
