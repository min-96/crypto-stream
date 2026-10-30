"""Parse RegionKorea_TickerPreprocessing values and assign 1-minute event-time windows (ADR-023)."""

import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from data_constructure import market_schema, ticker_data_schema  # noqa: E402  original schema (ADR-001)

FIELDS = tuple(field.name for field in ticker_data_schema.fields)

# Internal Decimal, never Double (ADR-023): 20 integer and 18 fractional digits.
DECIMAL_TYPE = DecimalType(38, 18)
_MAX_INTEGER_DIGITS = DECIMAL_TYPE.precision - DECIMAL_TYPE.scale
_MAX_FRACTIONAL_DIGITS = DECIMAL_TYPE.scale

# Plain decimal as upbit.py writes it with str(): optional sign, digits, optional exponent.
_PLAIN_DECIMAL = r'^[+-]?([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$'

# Epoch seconds before year 10000; rejects Infinity and millisecond/microsecond mix-ups.
_MAX_EPOCH_SECONDS = 253402300800


def _decimal_problem(text, field):
    """Null if `text` fits DECIMAL_TYPE exactly, else why it would be rounded, overflow or not parse.

    Spark's cast rounds extra fractional digits and turns overflow into null without an
    error, so the digit counts are checked on the string before casting.
    """
    int_part = F.regexp_extract(text, _PLAIN_DECIMAL, 1)
    frac_part = F.regexp_extract(text, _PLAIN_DECIMAL, 2)
    exp_text = F.regexp_extract(text, _PLAIN_DECIMAL, 3)
    exponent = F.when(exp_text == '', F.lit(0)).otherwise(exp_text.cast('int')).cast('bigint')
    digits = F.concat(int_part, frac_part)
    significant = F.regexp_replace(digits, '^0+', '')
    trailing_zeros = F.length(significant) - F.length(F.regexp_replace(significant, '0+$', ''))
    # value = int(digits) * 10 ** -shift; trailing zeros carry no value.
    shift = F.length(frac_part) - exponent
    is_zero = F.length(significant) == 0
    fractional_digits = F.when(is_zero, 0).otherwise(F.greatest(shift - trailing_zeros, F.lit(0)))
    integer_digits = F.when(is_zero, 0).otherwise(F.greatest(F.length(significant) - shift, F.lit(0)))
    plain = text.rlike(_PLAIN_DECIMAL) & (F.length(digits) > 0) & exponent.isNotNull()
    prefix = f'AVG-06: {field} '
    return (F.when(text.isNull(), F.lit(prefix + 'is missing'))
             .when(~plain, F.lit(prefix + 'is not a plain decimal number'))
             .when(fractional_digits > _MAX_FRACTIONAL_DIGITS,
                   F.lit(prefix + f'has more than {_MAX_FRACTIONAL_DIGITS} fractional digits'))
             .when(integer_digits > _MAX_INTEGER_DIGITS,
                   F.lit(prefix + f'has more than {_MAX_INTEGER_DIGITS} integer digits')))


def parse_preprocessed_ticker(df):
    """Kafka `value` → market, coin_symbol, event_time (Timestamp), 8 Decimal fields.

    Preprocessing (ADR-019) only publishes valid tickers, so anything unparsable or anything
    Decimal(38,18) cannot hold exactly is a contract violation and fails the query with the
    reason, instead of reaching the average as null or as a silently rounded value.
    """
    record = F.from_json(F.col('value').cast('string'), market_schema)
    raw = df.select(record.alias('r')).select(
        F.col('r.market').alias('market'),
        F.col('r.coin_symbol').alias('coin_symbol'),
        F.col('r.timestamp').alias('_seconds'),
        *(F.col(f'r.data.{field}').alias(f'_text_{field}') for field in FIELDS),
    )
    problems = {field: _decimal_problem(F.col(f'_text_{field}'), field) for field in FIELDS}
    # Only values that passed the digit checks are cast.
    parsed = raw.select(
        'market', 'coin_symbol', '_seconds',
        *(F.when(problems[field].isNull(), F.col(f'_text_{field}').cast(DECIMAL_TYPE)).alias(field)
          for field in FIELDS),
        *(problems[field].alias(f'_problem_{field}') for field in FIELDS),
    )
    seconds = F.col('_seconds')
    record_ok = F.coalesce(
        (F.length('market') > 0) & (F.length('coin_symbol') > 0)
        & seconds.isNotNull() & ~F.isnan(seconds) & (seconds > 0) & (seconds < _MAX_EPOCH_SECONDS),
        F.lit(False))
    problem = F.coalesce(
        F.when(~record_ok, F.lit('AVG-06: Preprocessed ticker identity or timestamp is invalid')),
        *(F.col(f'_problem_{field}') for field in FIELDS),
        *(F.when(F.col(field).isNull(), F.lit(f'AVG-06: {field} could not be converted'))
          for field in FIELDS),
    )
    # Sources are millisecond precision; rounding pins event_time to the ms grid instead of
    # keeping whatever sub-millisecond float noise a direct Double→Timestamp cast would.
    event_time = F.expr('timestamp_millis(CAST(round(_seconds * 1000) AS BIGINT))')
    return parsed.select(
        'market', 'coin_symbol',
        F.when(problem.isNull(), event_time).otherwise(F.raise_error(problem)).alias('event_time'),
        *FIELDS,
    )


def assign_minute_window(df):
    """Add the 1-minute tumbling `window` on event_time and `time` = window end in epoch ms."""
    return (df.withColumn('window', F.window('event_time', '1 minute'))
              .withColumn('time', F.expr('unix_millis(window.end)')))
