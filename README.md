# crypto-stream

암호화폐 거래소의 시세 데이터를 WebSocket으로 수집하고, Kafka에서 전처리한 뒤 Spark 집계 결과를 MySQL에 저장하는 로컬 데이터 파이프라인 프로젝트입니다. Kafka Connect는 전처리·분석 토픽을 MinIO에 보관합니다.

## 데이터 흐름

```text
거래소 WebSocket / 로컬 Upbit 모의 서버
  → Collector → Kafka Raw 토픽
  → 전처리 Consumer → Kafka Preprocessed 토픽
      ├─ Kafka Connect → MinIO preprocessed 버킷
      └─ Spark Structured Streaming → MySQL coin_average

Kafka Processed 토픽 → Kafka Connect → MinIO processed 버킷
```

Collector는 수신한 payload를 Raw 토픽에 그대로 발행합니다. 전처리 Consumer는 거래소별 값을 공통 스키마로 변환하고 Raw offset과 결과 발행을 Kafka 트랜잭션으로 묶습니다. 현재 평균 집계 작업은 한국 리전 Ticker 토픽(`RegionKorea_TickerPreprocessing`)을 읽어 1분 단위로 거래소별 평균을 구한 뒤, 거래소별 평균을 동일 가중치로 합산해 `coin_average` 테이블에 UPSERT합니다. 이벤트 시각을 기준으로 집계하며 10초 워터마크를 사용합니다.

## 현재 구현 범위

| 구성 요소 | 위치 | 역할 |
|---|---|---|
| Collector | `apps/collector/` | 설정된 WebSocket 소스의 Raw 메시지를 Kafka에 발행 |
| 전처리 | `apps/preprocessing/` | Upbit·Bithumb 변환기, 입력 검증, 트랜잭션 발행 |
| 평균 집계 | `apps/average/` | 한국 리전 Ticker의 1분 평균 계산과 MySQL 저장 |
| 로컬 인프라 | `infrastructure/` | Kafka KRaft 3노드, Connect, MySQL, MinIO, Spark |
| 검증 하네스 | `infrastructure/tests/kafka/` | 토픽·보관·장애 복구·파이프라인 검증 |

기본 Compose는 인프라를 실행합니다. Collector는 추가 Compose 파일을 사용합니다. 전처리 Consumer와 평균 집계 작업은 기본 Compose에서 상시 실행되지 않으며, 각 실행 코드와 검증 하네스를 통해 구동합니다. 여섯 개 분석 결과 토픽과 MySQL 분석 테이블은 인프라에 정의되어 있지만, 여섯 개 분석 작업 전체가 구현된 상태는 아닙니다.

## 로컬 실행

Docker Engine과 Docker Compose가 필요합니다. 최초 이미지 빌드 시 외부 이미지를 받고 Spark·Connect 의존성을 설치하므로 인터넷 연결과 충분한 디스크 공간이 필요합니다.

1. 로컬 환경 파일을 만들고 예시 자격 증명을 변경합니다.

   ```sh
   cp infrastructure/.env.example infrastructure/.env
   # infrastructure/.env의 MySQL·MinIO 자격 증명 수정
   ```

2. 저장소 루트에서 인프라를 시작합니다.

   ```sh
   docker compose --env-file infrastructure/.env \
     -f infrastructure/compose.yaml up -d --build
   docker compose --env-file infrastructure/.env \
     -f infrastructure/compose.yaml ps
   ```

3. 로컬 Upbit 모의 WebSocket과 Collector를 실행하려면 다음 Compose 파일을 추가합니다.

   ```sh
   docker compose --env-file infrastructure/.env \
     --env-file infrastructure/.env.mock-upbit.example \
     -f infrastructure/compose.yaml \
     -f infrastructure/compose.apps.yaml \
     -f infrastructure/compose.mock-upbit.yaml \
     up -d --build mock-upbit collector
   ```

   이 명령은 모의 Ticker를 Kafka Raw 토픽까지 발행합니다. 실제 거래소를 연결할 때는 `apps/collector/sources.upbit.json` 등의 소스 설정과 `infrastructure/.env.apps.example`을 참고하세요.

4. 인프라 상태를 확인합니다.

   ```sh
   cd infrastructure
   bash tests/smoke/check.sh
   bash spark/verify-connections.sh
   ```

Kafka 호스트 주소는 `localhost:19092,localhost:19093,localhost:19094`입니다. Spark UI는 `localhost:8080`, Kafka Connect API는 `localhost:8083`, MinIO API와 콘솔은 각각 `localhost:9000`, `localhost:9001`, MySQL은 `localhost:3306`에 바인딩됩니다.

볼륨을 보존하며 중지하려면 `docker compose --env-file infrastructure/.env -f infrastructure/compose.yaml down`을 사용합니다. MySQL 초기화 SQL은 새 데이터 볼륨을 만들 때만 실행됩니다.

## 테스트

```sh
python3 -m unittest discover -s apps/collector/tests -v
python3 -m unittest discover -s apps/preprocessing/tests -v
apps/average/run-spark-tests.sh
```

평균 집계 테스트 스크립트는 프로젝트 Spark 이미지를 사용하므로 먼저 인프라 이미지를 빌드해야 합니다. 일부 통합 테스트와 Kafka 검증 하네스는 Docker 컨테이너를 실행하거나 실행 중인 인프라를 요구합니다. 상세 실행 절차와 검증 범위는 `infrastructure/README.md`, `apps/collector/README.md`, `apps/preprocessing/README.md`를 참고하세요.

## 설계와 제약

- Raw 토픽은 `korea`, `ne`, `asia` 리전과 `ticker`, `orderbook` 종류로 나뉩니다. 기본 토픽은 1 partition, 복제 계수 3이며 운영 후보 프로필은 별도 설정입니다.
- 현재 Collector는 공통 WebSocket 전송기입니다. 거래소마다 다른 인증·구독 갱신·제어 프레임 처리 및 전체 종목 연결은 추가 구현과 검증이 필요합니다.
- `mysql_schema_draft.sql`의 여섯 분석 테이블은 초안입니다. 실제 평균 집계는 별도 `infrastructure/mysql/init/coin_average.sql`을 사용합니다.
- 데이터 계약, 설계 결정 및 검증 결과는 루트의 설계 문서, `plans/`, `docs/progress/`, `docs/evidence/`에 기록되어 있습니다.
