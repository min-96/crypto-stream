from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    FloatType,
    TimestampType,
    IntegerType,
    LongType,
)


time_metrics_schema = StructType(
    [
        StructField("region", StringType(), True),
        StructField("coin_symbol", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
        StructField("avg_price", DoubleType(), True),
        StructField("highest_price", DoubleType(), True),
        StructField("lowest_price", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("price_volatility", FloatType(), True),
        StructField("MA_price", DoubleType(), True),
        StructField("MA_volume", DoubleType(), True),
    ]
)


arbitrage_schema = StructType(
    [
        StructField("coin_symbol", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
        StructField("kr_price", DoubleType(), True),
        StructField("global_price", DoubleType(), True),
        StructField("asia_price", DoubleType(), True),
        StructField("kr_global_spread", DoubleType(), True),
        StructField("kr_asia_spread", DoubleType(), True),
        StructField("global_asia_spread", DoubleType(), True),
        StructField("kr_global_spread_percent", DoubleType(), True),
        StructField("kr_asia_spread_percent", DoubleType(), True),
        StructField("global_asia_spread_percent", DoubleType(), True),
    ]
)


orderbook_cal_schema = StructType(
    [
        StructField("region", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
        StructField("avg_highest_bid", DoubleType(), True),
        StructField("avg_lowest_ask", DoubleType(), True),
        StructField("total_bid_volume", DoubleType(), True),
        StructField("total_ask_volume", DoubleType(), True),
        StructField("record_count", IntegerType(), True),
    ]
)

orderbook_cal_all_schema = StructType(
    [
        StructField("region", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
        StructField("avg_highest_bid", DoubleType(), True),
        StructField("avg_lowest_ask", DoubleType(), True),
        StructField("total_bid_volume", DoubleType(), True),
        StructField("total_ask_volume", DoubleType(), True),
        StructField("record_count", IntegerType(), True),
    ]
)


market_arbitrage_schema = StructType(
    [
        StructField("coin_symbol", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
        # 거래소별 가격
        StructField("UPBIT_price", DoubleType(), True),
        StructField("BITHUMB_price", DoubleType(), True),
        StructField("KORBIT_price", DoubleType(), True),
        StructField("COINONE_price", DoubleType(), True),
        StructField("BYBIT_price", DoubleType(), True),
        StructField("GATEIO_price", DoubleType(), True),
        StructField("OKX_price", DoubleType(), True),
        StructField("BINANCE_price", DoubleType(), True),
        StructField("KRAKEN_price", DoubleType(), True),
        # 차익 계산 결과
        StructField("UPBIT_BITHUMB_spread", DoubleType(), True),
        StructField("UPBIT_KORBIT_spread", DoubleType(), True),
        StructField("UPBIT_COINONE_spread", DoubleType(), True),
        StructField("BYBIT_GATEIO_spread", DoubleType(), True),
        StructField("BYBIT_OKX_spread", DoubleType(), True),
        StructField("BINANCE_KRAKEN_spread", DoubleType(), True),
        StructField("UPBIT_BINANCE_spread", DoubleType(), True),
        StructField("UPBIT_BYBIT_spread", DoubleType(), True),
        StructField("BINANCE_BYBIT_spread", DoubleType(), True),
    ]
)

market_arbitrage_signal_schema = StructType(
    [
        StructField("coin_symbol", StringType(), True),
        StructField("market", StringType(), True),
        StructField("region", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("current_volume", DoubleType(), True),
        StructField("price_change_percent", DoubleType(), True),
        StructField("volume_change_percent", DoubleType(), True),
        StructField("trade_count", LongType(), True),
        StructField("price_volatility", DoubleType(), True),
        StructField("price_signal", StringType(), True),
        StructField("volume_signal", StringType(), True),
    ]
)
