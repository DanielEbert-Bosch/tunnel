
import asyncio
import websockets

MIDDLE_SERVER = "ws://sinflair.duckdns.org:3061"
VM_ID = "vm-1234"  # Dynamically assign this in your deployment script

async def handle_ssh(websocket):
    # Connect to the local SSH server
    reader, writer = await asyncio.open_connection('127.0.0.1', 22)

    async def ws_to_ssh():
        try:
            async for message in websocket:
                writer.write(message)
                await writer.drain()
        except Exception:
            writer.close()

    async def ssh_to_ws():
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                await websocket.send(data)
        except Exception:
            pass

    await asyncio.gather(ws_to_ssh(), ssh_to_ws())

async def main():
    while True:
        try:
            async with websockets.connect(MIDDLE_SERVER) as ws:
                await ws.send(f"VM:{VM_ID}")
                # Wait for the laptop to initiate the bridge
                await handle_ssh(ws)
        except Exception as e:
            print("Connection dropped, retrying in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
