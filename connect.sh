#!/usr/bin/env bash
set -euo pipefail

VM="${1:?usage: $0 <vm-name>}"

# --- write the (proven) tunnel server to a temp file -----------------------
SERVER="$(mktemp --suffix=.py)"
READY_FILE="$(mktemp)"
cleanup() {
    [[ -n "${TUN_PID:-}" ]] && kill "$TUN_PID" 2>/dev/null || true
    rm -f "$SERVER"
    rm -f "$READY_FILE"
}
trap cleanup EXIT

cat > "$SERVER" <<'PYEOF'
import asyncio, sys, websockets

MIDDLE_SERVER = "ws://sinflair.duckdns.org:3061"
TARGET_VM = sys.argv[1]
LISTEN_PORT = int(sys.argv[2])
READY_FILE = sys.argv[3]

async def handle_local_client(local_reader, local_writer):
    async with websockets.connect(MIDDLE_SERVER) as ws:
        await ws.send(f"LAPTOP:{TARGET_VM}")
        status = await ws.recv()
        if status != "CONNECTED":
            print(f"\n[tunnel] middle server refused: {status}", file=sys.stderr, flush=True)
            local_writer.close()
            return

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
    server = await asyncio.start_server(handle_local_client, "127.0.0.1", LISTEN_PORT)
    with open(READY_FILE, "w", encoding="utf-8") as f:
        f.write("READY\n")
    print("READY", flush=True)
    async with server:
        await server.serve_forever()

asyncio.run(main())
PYEOF

# --- pick a free local port ------------------------------------------------
PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"

# --- start tunnel in background, wait until it is listening -----------------
python3 "$SERVER" "$VM" "$PORT" "$READY_FILE" &
TUN_PID=$!

for _ in $(seq 1 50); do
    if [[ -s "$READY_FILE" ]]; then
        break
    fi
    if ! kill -0 "$TUN_PID" 2>/dev/null; then
        echo "[tunnel] local forwarder exited early" >&2
        exit 1
    fi
    sleep 0.1
done

if [[ ! -s "$READY_FILE" ]]; then
    echo "[tunnel] local forwarder did not become ready" >&2
    exit 1
fi

# --- connect -----------------------------------------------------------------
ssh -v -p "$PORT" azureuser@127.0.0.1
