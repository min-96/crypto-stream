"""Local Upbit/Bithumb-compatible WebSocket ticker stream for Collector tests."""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path


LOG = logging.getLogger(__name__)
TEMPLATE = Path(os.environ.get('MOCK_UPBIT_TEMPLATE', '/app/upbit_ticker.json'))
EXCHANGE = os.environ.get('MOCK_EXCHANGE', 'UPBIT')


def validate_subscription(message):
    try:
        request = json.loads(message)
    except (TypeError, ValueError) as error:
        raise ValueError('subscription must be JSON') from error
    if (not isinstance(request, list) or len(request) < 2
            or not isinstance(request[0], dict) or not request[0].get('ticket')
            or not any(isinstance(item, dict) and item.get('type') == 'ticker'
                       and item.get('codes') == ['KRW-BTC'] for item in request[1:])):
        raise ValueError('expected Upbit KRW-BTC ticker subscription')
    for item in request[1:]:
        if isinstance(item, dict) and item.get('format', 'DEFAULT') != 'DEFAULT':
            raise ValueError('mock supports only DEFAULT format')


def make_ticker(template, sequence, timestamp_ms):
    """Create a coherent snapshot/stream frame without using an external API."""
    frame = template.copy()
    previous = template['prev_closing_price']
    price = previous + ((sequence % 11) - 5) * 1000
    change = price - previous
    trade_at = datetime.fromtimestamp((timestamp_ms - 50) / 1000, timezone.utc)
    volume = round(0.001 + (sequence % 7) * 0.0001, 8)
    cycles, remainder = divmod(sequence + 1, 7)
    added_volume = cycles * 0.0091 + remainder * 0.001 + remainder * (remainder - 1) * 0.00005
    frame.update(
        type='ticker', code='KRW-BTC',
        high_price=max(template['high_price'], price),
        low_price=min(template['low_price'], price),
        trade_price=price,
        change='RISE' if change > 0 else 'FALL' if change < 0 else 'EVEN',
        change_price=abs(change), signed_change_price=change,
        change_rate=round(abs(change) / previous, 12),
        signed_change_rate=round(change / previous, 12),
        ask_bid='BID' if sequence % 2 else 'ASK',
        trade_volume=volume,
        acc_trade_volume=round(template['acc_trade_volume'] + added_volume, 8),
        acc_trade_volume_24h=round(template['acc_trade_volume_24h'] + added_volume, 8),
        acc_trade_price=round(template['acc_trade_price'] + added_volume * previous, 8),
        acc_trade_price_24h=round(template['acc_trade_price_24h'] + added_volume * previous, 8),
        trade_date=trade_at.strftime('%Y%m%d'),
        trade_time=trade_at.strftime('%H%M%S'),
        trade_timestamp=timestamp_ms - 50,
        timestamp=timestamp_ms,
        stream_type='SNAPSHOT' if sequence == 0 else 'REALTIME',
    )
    return frame


async def serve_ticker(socket, template, interval_seconds):
    try:
        validate_subscription(await asyncio.wait_for(socket.recv(), timeout=10))
    except (ValueError, asyncio.TimeoutError) as error:
        await socket.send(json.dumps({'error': str(error)}))
        await socket.close(code=1008, reason='invalid subscription')
        return
    LOG.info('%s KRW-BTC ticker subscribed', EXCHANGE)
    sequence = 0
    try:
        while True:
            timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            await socket.send(json.dumps(make_ticker(template, sequence, timestamp_ms),
                                         separators=(',', ':')))
            sequence += 1
            await asyncio.sleep(interval_seconds)
    except Exception as error:
        from websockets.exceptions import ConnectionClosed
        if not isinstance(error, ConnectionClosed):
            raise
        LOG.info('ticker subscriber disconnected')


async def main():
    from websockets import serve

    interval_ms = int(os.environ.get('MOCK_UPBIT_INTERVAL_MS', '1000'))
    if not 100 <= interval_ms <= 60000:
        raise ValueError('MOCK_UPBIT_INTERVAL_MS must be 100..60000')
    template = json.loads(TEMPLATE.read_text())
    async with serve(lambda socket: serve_ticker(socket, template, interval_ms / 1000),
                     '0.0.0.0', 8765):
        LOG.info('mock %s WebSocket listening on 8765', EXCHANGE)
        await asyncio.Future()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    asyncio.run(main())
