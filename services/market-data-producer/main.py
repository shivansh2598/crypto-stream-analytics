import asyncio
import json

import websockets


# Binance WebSocket URL for BTCUSDT trade stream
WEBSOCKET_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# How long to wait before reconnecting after a disconnect (seconds)
RECONNECT_DELAY = 3


async def consume_trades():
    """Connect to Binance trade stream, decode trade events, and print to terminal."""
    while True:  # keep trying to connect — Binance drops idle connections periodically
        try:
            # connect to the stream
            async with websockets.connect(WEBSOCKET_URL) as ws:
                print(f"Connected to {WEBSOCKET_URL}")
                # listen for trade events
                async for message in ws:
                    trade = json.loads(message)  # decode each raw message
                    # extract the fields we care about
                    symbol = trade.get("s")
                    price = trade.get("p")
                    quantity = trade.get("q")
                    trade_time = trade.get("T")  # milliseconds since epoch
                    print(f"[{symbol}] price={price} qty={quantity} time={trade_time}")
        except websockets.ConnectionClosed:
            print(f"Disconnected — reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)
        except Exception as e:
            print(f"Unexpected error: {e} — reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)


def main():
    asyncio.run(consume_trades())


if __name__ == "__main__":
    main()
