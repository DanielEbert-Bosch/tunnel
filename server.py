import asyncio
import websockets

OPEN_TOKEN = "__OPEN__"
CLOSE_TOKEN = "__CLOSE__"

# Dictionary to hold connected VMs: {"vm_id": websocket_connection}
vms = {}
# VMs that currently have an active laptop bridge. A single vm_ws can only
# carry one SSH stream at a time, so we reject a second laptop for the same VM.
busy = set()

async def handler(websocket):
    # Determine if connecting client is a VM or the Laptop
    client_type = await websocket.recv()
    if isinstance(client_type, bytes):
        client_type = client_type.decode("utf-8", errors="replace")
    client_type = client_type.strip()

    if client_type.startswith("VM:"):
        vm_id = client_type.split(":", 1)[1].strip()
        if not vm_id:
            await websocket.close(code=1008, reason="invalid vm id")
            return
        vms[vm_id] = websocket
        print(f"VM Registered: {vm_id}")
        try:
            await websocket.wait_closed()
        finally:
            # Only remove if this socket is still the registered one
            if vms.get(vm_id) is websocket:
                del vms[vm_id]
            print(f"VM Disconnected: {vm_id}")

    elif client_type.startswith("LAPTOP:"):
        target_vm = client_type.split(":", 1)[1].strip()
        if not target_vm:
            await websocket.send("ERROR: Invalid VM id")
            return
        vm_ws = vms.get(target_vm)
        if vm_ws is None:
            await websocket.send("ERROR: VM not found")
            return

        # One SSH stream per VM: a single vm_ws cannot multiplex two laptops.
        if target_vm in busy:
            await websocket.send("ERROR: VM busy")
            return
        busy.add(target_vm)

        try:
            await vm_ws.send(OPEN_TOKEN)
        except websockets.exceptions.ConnectionClosed:
            if vms.get(target_vm) is vm_ws:
                del vms[target_vm]
            busy.discard(target_vm)
            await websocket.send("ERROR: VM disconnected")
            return

        await websocket.send("CONNECTED")
        print(f"Laptop bridged to {target_vm}")

        async def forward_l2v():
            try:
                async for message in websocket:
                    if isinstance(message, bytes):
                        await vm_ws.send(message)
            except websockets.exceptions.ConnectionClosed:
                pass

        async def forward_v2l():
            try:
                async for message in vm_ws:
                    if isinstance(message, bytes):
                        await websocket.send(message)
            except websockets.exceptions.ConnectionClosed:
                pass

        task_l2v = asyncio.ensure_future(forward_l2v())
        task_v2l = asyncio.ensure_future(forward_v2l())

        # As soon as EITHER direction ends (e.g. the laptop disconnects),
        # cancel the other so we never leave a dangling recv() on the
        # persistent vm_ws. Leaving it dangling is what caused the
        # ConcurrencyError ("cannot call recv while another coroutine is
        # already running recv") on the next laptop connection.
        try:
            done, pending = await asyncio.wait(
                {task_l2v, task_v2l},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            # Let cancellation settle so vm_ws is free for the next laptop.
            await asyncio.gather(*pending, return_exceptions=True)
        finally:
            try:
                await vm_ws.send(CLOSE_TOKEN)
            except websockets.exceptions.ConnectionClosed:
                pass
            busy.discard(target_vm)
        print(f"Laptop disconnected from {target_vm}")

async def main():
    async with websockets.serve(handler, "0.0.0.0", 3061):
        print("Middle Server running on port 3061...")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
