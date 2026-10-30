"""First-stage aggregation: per (1-minute window, exchange, coin) sums and counts (ADR-024)."""

from pyspark.sql import functions as F

from ticker_input import DECIMAL_TYPE, FIELDS, assign_minute_window

WATERMARK_DELAY = '10 seconds'


def aggregate_exchange_window(parsed):
    """Parsed tickers → time, market, coin_symbol, message_count, <field>_sum per closed window.

    Only sums and counts: Spark's `avg` on Decimal(38,18) keeps 16 integer digits, so the
    division and its rounding happen in AVG-08. In Append mode a window is emitted once, in
    the batch after the watermark reaches its end; rows for an already emitted window are
    dropped (numRowsDroppedByWatermark).

    `sum` turns a Decimal overflow into null without an error, and inputs are never null
    (parse_preprocessed_ticker), so a null sum means overflow and fails the query.
    """
    # The watermark must be set before the window is derived so the window carries it.
    windowed = assign_minute_window(parsed.withWatermark('event_time', WATERMARK_DELAY))
    aggregated = windowed.groupBy('window', 'time', 'market', 'coin_symbol').agg(
        F.count(F.lit(1)).alias('message_count'),
        *(F.sum(field).alias(f'_sum_{field}') for field in FIELDS),
    )
    limit = f'{DECIMAL_TYPE.precision - DECIMAL_TYPE.scale} integer digits'
    return aggregated.select(
        'time', 'market', 'coin_symbol', 'message_count',
        *(F.coalesce(F.col(f'_sum_{field}'),
                     F.raise_error(F.lit(f'AVG-07: {field} sum is out of Decimal range ({limit})')))
          .alias(f'{field}_sum')
          for field in FIELDS),
    )
