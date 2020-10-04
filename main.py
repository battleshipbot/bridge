"""
Created by Epic at 10/4/20
"""

from websockets import serve, WebSocketServerProtocol
from asyncio import get_event_loop, Lock
from ujson import loads, dumps
from logging import getLogger, basicConfig, DEBUG, WARNING

# Listeners
listeners = []
select_listeners = {}

# Logging
logger = getLogger("bridge.server")
basicConfig(level=WARNING)
logger.setLevel(DEBUG)

# Locks
connection_lock = Lock()


async def broadcast(message, path="*"):
    message["p"] = path
    encoded = dumps(message)
    if path == "*":
        receivers = listeners
    else:
        receivers = select_listeners.get(path, [])
    for listener in receivers:
        await listener.send(encoded)


async def server(websocket: WebSocketServerProtocol, _):
    logger.info(f"A service connected.")
    server_name = None
    listeners.append(websocket)
    async for message in websocket:
        parsed = loads(message)
        if parsed["op"] == 0:
            async with connection_lock:
                server_name = parsed["d"]
                subscribe_paths = parsed["p"]
                if type(subscribe_paths) != list:
                    await websocket.close(reason=f"Paths has to be a list. Not a {type(subscribe_paths)}")
                for path in subscribe_paths:
                    current_paths = select_listeners.get(path, [])
                    current_paths.append(websocket)
                    select_listeners[path] = current_paths

                logger.info(f"Service '{parsed['d']}' connected.")
                await broadcast({"op": 3, "d": server_name})
                continue

        if parsed["op"] == 1 and server_name is None:
            await websocket.close(reason="Please register before dispatching")
            continue

        if parsed["op"] == 1:
            await broadcast({"op": 2, "d": parsed["d"], "e": parsed["e"]}, path=parsed.get("p", "*"))

    listeners.remove(websocket)
    async with connection_lock:
        for path in subscribe_paths:
            current_paths = select_listeners.get(path, [])
            current_paths.remove(websocket)
            if len(current_paths) == 0:
                del select_listeners[path]
            else:
                select_listeners[path] = current_paths

    logger.info(f"Service '{server_name}' disconnected.")
    await broadcast({"op": 4, "d": server_name})


start_server = serve(server, "0.0.0.0", 5000)

loop = get_event_loop()
loop.run_until_complete(start_server)
loop.run_forever()
