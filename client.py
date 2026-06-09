import asyncio
import websockets
import sys

MIDDLE_SERVER = "ws://sinflair.duckdns.org:3061"
TARGET_VM = sys.argv[1] # e.g., run as: python laptop.py vm-1234

async def handle_local_client(local_reader, local_writer):
    async with websockets.connect(MIDDLE_SERVER) as ws:
        await ws.send(f"LAPTOP:{TARGET_VM}")
        status = await ws.recv()
        if status != "CONNECTED":
            print(f"Failed to connect: {status}")
            local_writer.close()
            return
          
        print(f"Tunnel established to {TARGET_VM}")

        async def local_to_ws():
            try:
                while True:
                    data = await local_reader.read(4096)
                    if not data:
                        break
                    await ws.send(data)
            except Exception:
                pass

        async def ws_to_local():
            try:
                async for message in ws:
                    local_writer.write(message)
                    await local_writer.drain()
            except Exception:
                local_writer.close()

        await asyncio.gather(local_to_ws(), ws_to_local())

async def main():
    server = await asyncio.start_server(handle_local_client, '127.0.0.1', 3061)
    print(f"Listening on 127.0.0.1:3061... routing to {TARGET_VM}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
