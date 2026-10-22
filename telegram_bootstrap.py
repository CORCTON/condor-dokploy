"""Deployment wrapper only: bind the local Agent and bootstrap its official Routine."""

import asyncio
import logging
import os
import sys

sys.path.insert(0, "/app")
log = logging.getLogger(__name__)


def bind():
    from condor.preferences import get_chat_binding, load_user_data_for, set_chat_binding

    data = load_user_data_for(int(os.environ["ADMIN_USER_ID"]))
    if get_chat_binding(data).get("agent_slug") != "telegram_agent":
        set_chat_binding(data, {"agent_slug": "telegram_agent", "conversation_id": ""})


async def bootstrap():
    import aiohttp
    from condor.web.auth import create_jwt
    from utils.config import WEB_PORT

    async with aiohttp.ClientSession() as session:
        for _ in range(120):
            try:
                headers = {
                    "Authorization": "Bearer "
                    + create_jwt(int(os.environ["ADMIN_USER_ID"]), role="user")
                }
                base = f"http://127.0.0.1:{WEB_PORT}/api/v1/routines"
                async with session.get(
                    base + "/instances", headers=headers, timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    response.raise_for_status()
                    instances = await response.json()
                if any(
                    item.get("routine_name") == "telegram_agent/exit_planner"
                    and item.get("status") == "running"
                    for item in instances
                ):
                    return
                async with session.post(
                    base + "/start",
                    headers=headers,
                    json={
                        "routine_name": "telegram_agent/exit_planner",
                        "server_name": "main",
                        "config": {},
                        "attribute_to": "telegram_agent",
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    response.raise_for_status()
                log.warning("TelegramAgent exit planner started via official Routine API")
                return
            except Exception as exc:  # noqa: BLE001 -- retry only local bootstrap, never exchange writes
                log.warning("TelegramAgent bootstrap waiting: %s", type(exc).__name__)
                await asyncio.sleep(5)
        raise RuntimeError("TelegramAgent bootstrap did not complete")


if __name__ == "__main__":
    if "--bind" in sys.argv:
        bind()
    else:
        asyncio.run(bootstrap())
