-- A 대표 평균 결과(average_schema). ADR-018·023·024.
-- 6개 분석 테이블(mysql_schema_draft.sql)과 별도 테이블이다.
-- time: 거래소 이벤트 시각 기준 1분 Window 종료 시각, Unix epoch 밀리초(ADR-023). 60000의 배수.
-- 8개 필드: average_schema의 StringType 그대로, Decimal을 지수 표기 없이 쓴 문자열.
-- 거래소가 0곳인 코인·Window는 행을 만들지 않으므로 모든 필드가 NOT NULL이다(ADR-017).
CREATE TABLE IF NOT EXISTS `coin_average` (
    `name` VARCHAR(32) NOT NULL,
    `time` BIGINT NOT NULL,
    `opening_price` VARCHAR(64) NOT NULL,
    `trade_price` VARCHAR(64) NOT NULL,
    `max_price` VARCHAR(64) NOT NULL,
    `min_price` VARCHAR(64) NOT NULL,
    `prev_closing_price` VARCHAR(64) NOT NULL,
    `acc_trade_volume_24h` VARCHAR(64) NOT NULL,
    `signed_change_price` VARCHAR(64) NOT NULL,
    `signed_change_rate` VARCHAR(64) NOT NULL,
    PRIMARY KEY (`name`, `time`),
    CONSTRAINT `coin_average_time_window_end_ms`
        CHECK (`time` >= 1000000000000 AND `time` % 60000 = 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
