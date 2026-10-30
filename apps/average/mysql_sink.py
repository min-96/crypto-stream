"""MySQL sink contract for the A representative average (`coin_average`)."""

from pathlib import Path

DDL_PATH = Path(__file__).resolve().parents[2] / 'infrastructure' / 'mysql' / 'init' / 'coin_average.sql'

FIELDS = (
    'opening_price', 'trade_price', 'max_price', 'min_price', 'prev_closing_price',
    'acc_trade_volume_24h', 'signed_change_price', 'signed_change_rate',
)

_COLUMNS = ('name', 'time', *FIELDS)

WINDOW_MS = 60000

# `?` placeholders work for both JDBC PreparedStatement and MySQL server-side PREPARE.
UPSERT_SQL = (
    'INSERT INTO `coin_average` (' + ', '.join(f'`{c}`' for c in _COLUMNS) + ') '
    'VALUES (' + ', '.join('?' for _ in _COLUMNS) + ') AS new '
    'ON DUPLICATE KEY UPDATE ' + ', '.join(f'`{f}` = new.`{f}`' for f in FIELDS)
)


def upsert_params(row):
    """Return UPSERT_SQL parameters for one average_schema row, rejecting contract violations."""
    name, time_ms, data = row.get('name'), row.get('time'), row.get('data')
    if not isinstance(name, str) or not name:
        raise ValueError('average name must be a non-empty string')
    # ADR-023: end of the 1-minute event-time window, in epoch milliseconds.
    if type(time_ms) is not int or time_ms < 1_000_000_000_000 or time_ms % WINDOW_MS:
        raise ValueError('average time must be a 1-minute window end in epoch milliseconds')
    if not isinstance(data, dict):
        raise ValueError('average data must be an object')
    values = []
    for field in FIELDS:
        value = data.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f'average {field} must be a non-empty string')
        values.append(value)
    return (name, time_ms, *values)
