
import asyncio
import websockets

MIDDLE_SERVER = "ws://sinflair.duckdns.org:3061"
VM_ID = "vm-1234"  # Dynamically assign this in your deployment script
OPEN_TOKEN = "__OPEN__"
CLOSE_TOKEN = "__CLOSE__"

async def handle_ssh(websocket):
    async def run_single_session():
        ssh_reader, ssh_writer = await asyncio.open_connection("127.0.0.1", 22)

        async def ws_to_ssh():
            try:
                async for message in websocket:
                    if isinstance(message, str):
                        if message == CLOSE_TOKEN:
                            break
                        continue
                    ssh_writer.write(message)
                    await ssh_writer.drain()
            except websockets.exceptions.ConnectionClosed:
                pass

        async def ssh_to_ws():
            try:
                while True:
                    data = await ssh_reader.read(4096)
                    if not data:
                        break
                    await websocket.send(data)
            except websockets.exceptions.ConnectionClosed:
                pass

        task_ws_to_ssh = asyncio.ensure_future(ws_to_ssh())
        task_ssh_to_ws = asyncio.ensure_future(ssh_to_ws())

        done, pending = await asyncio.wait(
            {task_ws_to_ssh, task_ssh_to_ws},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        ssh_writer.close()
        try:
            await ssh_writer.wait_closed()
        except Exception:
            pass

    while True:
        message = await websocket.recv()
        if isinstance(message, str) and message == OPEN_TOKEN:
            try:
                await run_single_session()
            except Exception:
                pass

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
