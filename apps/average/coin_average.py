"""Second-stage aggregation inside foreachBatch and the coin_average UPSERT (ADR-024)."""

from decimal import Decimal
from fractions import Fraction

from mysql_sink import FIELDS, UPSERT_SQL, upsert_params

# AVG-08 rounding: the coin average is computed exactly as a fraction and rounded once, to the
# 18 fractional digits of Decimal(38,18), half to even. Rounding only at the end keeps the
# result independent of row order and of how the exchange averages would have been rounded.
SCALE = 18


def _plain_string(value):
    """Exact Fraction → fixed 18-digit decimal string without exponent; 0 is never '-0'."""
    units = round(value * 10 ** SCALE)  # Fraction rounds half to even, without a context
    return format(Decimal(f'{units}E-{SCALE}'), 'f')


def _exchange_averages(row):
    """One AVG-07 row → exact per-field average sum / message_count."""
    count = row['message_count']
    if type(count) is not int or count <= 0:
        raise ValueError(f'AVG-08: message_count must be a positive integer, got {count!r}')
    averages = {}
    for field in FIELDS:
        total = row[f'{field}_sum']
        if not isinstance(total, Decimal) or not total.is_finite():
            raise ValueError(f'AVG-08: {field}_sum must be a finite Decimal, got {total!r}')
        averages[field] = Fraction(total) / count
    return averages


def coin_averages(rows):
    """AVG-07 rows of one batch → average_schema dicts sorted by (name, time).

    name is the coin_symbol. Every exchange with a row for the (coin, window) has one equal
    vote; a (coin, window) without any exchange row has no output. Spark's avg is not used:
    on Decimal(38,18) it keeps only 16 integer digits.
    """
    windows = {}
    for row in rows:
        exchanges = windows.setdefault((row['coin_symbol'], row['time']), {})
        market = row['market']
        if market in exchanges:
            raise ValueError(f'AVG-08: {market} has two rows for {row["coin_symbol"]} at {row["time"]}')
        exchanges[market] = _exchange_averages(row)
    results = []
    for (name, time_ms), exchanges in sorted(windows.items()):
        data = {field: _plain_string(sum(avg[field] for avg in exchanges.values()) / len(exchanges))
                for field in FIELDS}
        result = {'name': name, 'time': time_ms, 'data': data}
        upsert_params(result)  # contract check before anything is written
        results.append(result)
    return results


def batch_writer(connect):
    """foreachBatch function: UPSERT the batch's coin averages in one JDBC transaction.

    `connect` returns a java.sql.Connection. Re-running a batch writes the same (name, time)
    and values, and a failed write rolls back and fails the batch so Spark retries it.
    """
    def write_batch(batch, batch_id):
        params = [upsert_params(average) for average in coin_averages(batch.collect())]
        if not params:
            return
        connection = connect()
        try:
            connection.setAutoCommit(False)
            statement = connection.prepareStatement(UPSERT_SQL)
            try:
                for name, time_ms, *values in params:
                    statement.setString(1, name)
                    statement.setLong(2, time_ms)
                    for index, value in enumerate(values, 3):
                        statement.setString(index, value)
                    statement.addBatch()
                statement.executeBatch()
            finally:
                statement.close()
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        print(f'AVG-08 batch={batch_id} coin_windows={len(params)}', flush=True)
    return write_batch


def jdbc_connector(spark, url, user, password):
    """Connection factory on the Driver JVM; the image ships the MySQL Connector/J jar."""
    jvm = spark.sparkContext._jvm
    jvm.java.lang.Class.forName('com.mysql.cj.jdbc.Driver')
    return lambda: jvm.java.sql.DriverManager.getConnection(url, user, password)
