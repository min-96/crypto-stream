"""Normalize the supplied Upbit KRW WebSocket ticker and orderbook frames."""

import json
import math
from datetime import datetime, timezone


def _frame(raw, key, kind):
    if not isinstance(raw, bytes) or not isinstance(key, bytes):
        raise ValueError('Upbit frame and Kafka key must be bytes')
    try:
        frame = json.loads(raw)
        exchange, coin, quote = key.decode('ascii').split(':')
        frame_quote, frame_coin = frame['code'].split('-')
    except (UnicodeDecodeError, ValueError, KeyError, TypeError, AttributeError) as error:
        raise ValueError('invalid Upbit frame or Kafka key') from error
    if (not isinstance(frame, dict) or frame.get('type') != kind or exchange != 'UPBIT'
            or quote != 'KRW' or frame_quote != quote or frame_coin != coin):
        raise ValueError('Upbit frame type or trading pair does not match Kafka key')
    return frame


def _number(frame, name, *, positive=False):
    value = frame.get(name)
    if (not isinstance(value, (int, float)) or isinstance(value, bool)
            or not math.isfinite(value) or (positive and value <= 0)):
        raise ValueError(f'invalid Upbit {name}')
    return value


def _ticker_value(frame, name):
    """Pass the value to the common Ticker checks; only the JSON shape is enforced here."""
    if name not in frame:
        raise ValueError(f'missing Upbit {name}')
    value = frame[name]
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f'Upbit {name} must be a JSON number or null')
    return str(value)


def _timestamp(frame):
    value = _number(frame, 'timestamp', positive=True)
    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as error:
        raise ValueError('invalid Upbit timestamp') from error


def convert_ticker(raw, key):
    frame = _frame(raw, key, 'ticker')
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
    return {'market': 'UPBIT', 'timestamp': _timestamp(frame).timestamp(),
            'coin_symbol': key.decode('ascii').split(':')[1], 'data': data}


def convert_orderbook(raw, key):
    frame = _frame(raw, key, 'orderbook')
    units = frame.get('orderbook_units')
    if not isinstance(units, list) or not 1 <= len(units) <= 1000:
        raise ValueError('invalid Upbit orderbook units')
    for unit in units:
        if not isinstance(unit, dict):
            raise ValueError('invalid Upbit orderbook unit')
        for name in ('ask_price', 'bid_price', 'ask_size', 'bid_size'):
            _number(unit, name, positive=name.endswith('_price'))
    highest_bid = max(unit['bid_price'] for unit in units)
    lowest_ask = min(unit['ask_price'] for unit in units)
    total_bid = _number(frame, 'total_bid_size')
    total_ask = _number(frame, 'total_ask_size')
    if total_bid < 0 or total_ask < 0:
        raise ValueError('negative orderbook total volume')
    return [{'region': 'Korea', 'market': 'UPBIT',
             'symbol': key.decode('ascii').split(':')[1],
             'highest_bid': highest_bid, 'lowest_ask': lowest_ask,
             'total_bid_volume': total_bid, 'total_ask_volume': total_ask,
             'timestamp': _timestamp(frame).isoformat(timespec='microseconds').replace('+00:00', 'Z')}]
