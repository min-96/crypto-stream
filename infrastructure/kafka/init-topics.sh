#!/bin/bash
set -euo pipefail
bootstrap=kafka-1:9092
mode=${1:-init}
profile=${KAFKA_TOPIC_PROFILE:-legacy}
if [[ "$mode" != init && "$mode" != --check ]]; then
  echo "usage: init-topics.sh [--check]" >&2
  exit 2
fi
if [[ "$profile" != legacy && "$profile" != operational-candidate ]]; then
  echo "KAFKA_TOPIC_PROFILE must be legacy or operational-candidate" >&2
  exit 2
fi
errors=0
check_topic() {
  local topic=$1 expected_partitions=$2 compact=$3 description
  if ! description=$(kafka-topics --bootstrap-server "$bootstrap" --describe --topic "$topic" 2>&1); then
    echo "MISSING topic=$topic: $description" >&2
    errors=$((errors + 1))
    return
  fi
  local header=${description%%$'\n'*}
  if [[ "$header" != *"PartitionCount: $expected_partitions"* || "$header" != *"ReplicationFactor: 3"* ]]; then
    echo "MISMATCH topic=$topic expected partitions=$expected_partitions RF=3: $header" >&2
    errors=$((errors + 1))
  fi
  if [[ "$compact" == yes && "$header" != *"cleanup.policy=compact"* ]]; then
    echo "MISMATCH topic=$topic expected cleanup.policy=compact: $header" >&2
    errors=$((errors + 1))
  fi
}
for topic in koreaSocketDataInBTC-ticker koreaSocketDataInBTC-orderbook neSocketDataInBTC-ticker neSocketDataInBTC-orderbook asiaSocketDataInBTC-ticker asiaSocketDataInBTC-orderbook RegionKorea_TickerPreprocessing RegionKorea_OrderbookPreprocessing RegionNE_TickerPreprocessing RegionNE_OrderbookPreprocessing RegionAsia_TickerPreprocessing RegionAsia_OrderbookPreprocessing TimeMetricsProcessedCoin ArbitrageProcessedCoin OrderbookProcessedRegionCoin OrderbookProcessedAllRegionCoin MarketArbitrageProcessedCoin MarketArbitrageSignalProcessedCoin; do
  partitions=1
  if [[ "$profile" == operational-candidate ]]; then
    case "$topic" in
      *-ticker|*_TickerPreprocessing) partitions=3 ;;
      *-orderbook|*_OrderbookPreprocessing) partitions=6 ;;
    esac
  fi
  if [[ "$mode" == init ]]; then
    kafka-topics --bootstrap-server "$bootstrap" --create --if-not-exists --topic "$topic" --partitions "$partitions" --replication-factor 3
  fi
  check_topic "$topic" "$partitions" no
done
for topic in connect-configs connect-offsets connect-status; do
  if [[ "$mode" == init ]]; then
    kafka-topics --bootstrap-server "$bootstrap" --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 3 --config cleanup.policy=compact
  fi
  check_topic "$topic" 1 yes
done
if (( errors > 0 )); then
  echo "$errors topic contract mismatch(es); existing partitions were not changed" >&2
  exit 1
fi
