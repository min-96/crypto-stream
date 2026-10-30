"""
pyspark data schema
{
    "upbit": {
        "name": "upbit-ETH",
        "timestamp": 1689633864.89345,
        "data": {
            "opening_price": 2455000.0,
            "max_price": 2462000.0,
            "min_price": 2431000.0,
            "prev_closing_price": 2455000.0,
            "acc_trade_volume_24h": 11447.92825886,
        }
    },
    .....
}

"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    ArrayType,
    FloatType,
    TimestampType,
)

orderbook_data_schema = ArrayType(
    StructType(
        [
            StructField("region", StringType(), True),
            StructField("market", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("highest_bid", FloatType(), True),
            StructField("lowest_ask", FloatType(), True),
            StructField("total_bid_volume", FloatType(), True),
            StructField("total_ask_volume", FloatType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )
)

ticker_data_schema = StructType(
    [
        StructField("opening_price", StringType(), True),
        StructField("trade_price", StringType(), True),
        StructField("max_price", StringType(), True),
        StructField("min_price", StringType(), True),
        StructField("prev_closing_price", StringType(), True),
        StructField("acc_trade_volume_24h", StringType(), True),
        StructField("signed_change_price", StringType(), True),
        StructField("signed_change_rate", StringType(), True),
    ]
)

market_schema = StructType(
    [
        StructField("market", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("coin_symbol", StringType(), True),
        StructField("data", ticker_data_schema),
    ]
)

korea_schema = StructType(
    [
        StructField("upbit", market_schema, True),
        StructField("bithumb", market_schema, True),
        StructField("coinone", market_schema, True),
        StructField("korbit", market_schema, True),
    ]
)


# 전체 마켓 데이터를 위한 스키마
socket_market_schema = ArrayType(
    StructType(
        [
            StructField("region", StringType(), True),
            StructField("market", StringType(), True),
            StructField("coin_symbol", StringType(), True),
            StructField("timestamp", DoubleType(), True),
            StructField("data", ArrayType(ticker_data_schema), True),
        ]
    )
)

"""
# 평균값
{
    "name": "ETH",
    "timestamp": 1689633864.89345,
    "data": {
        "opening_price": 2455000.0,
        "max_price": 2462000.0,
        "min_price": 2431000.0,
        "prev_closing_price": 2455000.0,
        "acc_trade_volume_24h": 11447.92825886,
    }
}
"""
average_schema = StructType(
    StructType(
        [
            StructField("name", StringType()),
            StructField("time", LongType()),
            StructField("data", ticker_data_schema),
        ]
    ),
)
