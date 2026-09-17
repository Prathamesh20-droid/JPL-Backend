import socketio

from core.utils import get_local_ip

FRONTEND_PORT = 3000
local_ip = get_local_ip()
team_sockets = {}

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins=[
        f"http://localhost:{FRONTEND_PORT}",
        f"http://127.0.0.1:{FRONTEND_PORT}",
        f"http://{local_ip}:{FRONTEND_PORT}",
        "https://jpl-frontend.vercel.app"
    ],
    logger=True,
    engineio_logger=True
)