-- v0.3 DDL 초안. preprocess_schema.py 필드/순서/대응 타입 유지.
-- PK, UNIQUE, INDEX, NOT NULL 없음: 원본에 없는 정책을 확정하지 않기 위함.
-- DATETIME(3) 시간대 및 DOUBLE 정밀도 정책은 미결.

-- time_metrics: 지역·코인·시간 구간의 가격/거래량 통계
CREATE TABLE IF NOT EXISTS `time_metrics` (
    `region` VARCHAR(255) NULL,
    `coin_symbol` VARCHAR(255) NULL,
    `start` DATETIME(3) NULL,
    `end` DATETIME(3) NULL,
    `avg_price` DOUBLE NULL,
    `highest_price` DOUBLE NULL,
    `lowest_price` DOUBLE NULL,
    `total_volume` DOUBLE NULL,
    `price_volatility` FLOAT NULL,
    `MA_price` DOUBLE NULL,
    `MA_volume` DOUBLE NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- arbitrage: 코인·시간 구간의 지역별 가격 차이
CREATE TABLE IF NOT EXISTS `arbitrage` (
    `coin_symbol` VARCHAR(255) NULL,
    `start` DATETIME(3) NULL,
    `end` DATETIME(3) NULL,
    `kr_price` DOUBLE NULL,
    `global_price` DOUBLE NULL,
    `asia_price` DOUBLE NULL,
    `kr_global_spread` DOUBLE NULL,
    `kr_asia_spread` DOUBLE NULL,
    `global_asia_spread` DOUBLE NULL,
    `kr_global_spread_percent` DOUBLE NULL,
    `kr_asia_spread_percent` DOUBLE NULL,
    `global_asia_spread_percent` DOUBLE NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- order_cal_region: 지역·코인·시간 구간의 호가 통계
CREATE TABLE IF NOT EXISTS `order_cal_region` (
    `region` VARCHAR(255) NULL,
    `symbol` VARCHAR(255) NULL,
    `start` DATETIME(3) NULL,
    `end` DATETIME(3) NULL,
    `avg_highest_bid` DOUBLE NULL,
    `avg_lowest_ask` DOUBLE NULL,
    `total_bid_volume` DOUBLE NULL,
    `total_ask_volume` DOUBLE NULL,
    `record_count` INT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- order_cal_all: 전체 지역·코인·시간 구간의 호가 통계
CREATE TABLE IF NOT EXISTS `order_cal_all` (
    `region` VARCHAR(255) NULL,
    `symbol` VARCHAR(255) NULL,
    `start` DATETIME(3) NULL,
    `end` DATETIME(3) NULL,
    `avg_highest_bid` DOUBLE NULL,
    `avg_lowest_ask` DOUBLE NULL,
    `total_bid_volume` DOUBLE NULL,
    `total_ask_volume` DOUBLE NULL,
    `record_count` INT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- market_arbitrage: 코인·시간 구간의 거래소별 가격 차이
CREATE TABLE IF NOT EXISTS `market_arbitrage` (
    `coin_symbol` VARCHAR(255) NULL,
    `start` DATETIME(3) NULL,
    `end` DATETIME(3) NULL,
    `UPBIT_price` DOUBLE NULL,
    `BITHUMB_price` DOUBLE NULL,
    `KORBIT_price` DOUBLE NULL,
    `COINONE_price` DOUBLE NULL,
    `BYBIT_price` DOUBLE NULL,
    `GATEIO_price` DOUBLE NULL,
    `OKX_price` DOUBLE NULL,
    `BINANCE_price` DOUBLE NULL,
    `KRAKEN_price` DOUBLE NULL,
    `UPBIT_BITHUMB_spread` DOUBLE NULL,
    `UPBIT_KORBIT_spread` DOUBLE NULL,
    `UPBIT_COINONE_spread` DOUBLE NULL,
    `BYBIT_GATEIO_spread` DOUBLE NULL,
    `BYBIT_OKX_spread` DOUBLE NULL,
    `BINANCE_KRAKEN_spread` DOUBLE NULL,
    `UPBIT_BINANCE_spread` DOUBLE NULL,
    `UPBIT_BYBIT_spread` DOUBLE NULL,
    `BINANCE_BYBIT_spread` DOUBLE NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- market_arbitrage_signal: 거래소·코인·시간 구간의 변화 신호
CREATE TABLE IF NOT EXISTS `market_arbitrage_signal` (
    `coin_symbol` VARCHAR(255) NULL,
    `market` VARCHAR(255) NULL,
    `region` VARCHAR(255) NULL,
    `start` DATETIME(3) NULL,
    `end` DATETIME(3) NULL,
    `current_price` DOUBLE NULL,
    `current_volume` DOUBLE NULL,
    `price_change_percent` DOUBLE NULL,
    `volume_change_percent` DOUBLE NULL,
    `trade_count` BIGINT NULL,
    `price_volatility` DOUBLE NULL,
    `price_signal` VARCHAR(255) NULL,
    `volume_signal` VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
