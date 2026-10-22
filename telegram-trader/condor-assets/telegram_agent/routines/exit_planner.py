"""Continuous exit planner. Restored through Condor's official routine store."""

import asyncio
import json
import logging
import os

import aiohttp
from pydantic import BaseModel

CONTINUOUS = True
log = logging.getLogger(__name__)


class Config(BaseModel):
    pass


async def run(config: Config, context):
    from condor.web.auth import create_jwt
    from utils.config import WEB_PORT

    trader_url = os.environ["TELEGRAM_TRADER_URL"].rstrip("/")
    trader_headers = {"Authorization": "Bearer " + os.environ["TRADER_TOKEN"]}
    owner = int(os.environ["ADMIN_USER_ID"])
    async with aiohttp.ClientSession() as session:
        while True:
            job = None
            try:
                # No model call and no exchange job claim before provider configuration.
                if os.getenv("TRADER_LLM_READY") != "true":
                    await asyncio.sleep(10)
                    continue
                async with session.post(
                    trader_url + "/jobs/claim",
                    headers=trader_headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    response.raise_for_status()
                    job = await response.json()
                if not job:
                    await asyncio.sleep(1)
                    continue
                prompt = "EXIT_PLAN_JOB\nReturn only the prescribed JSON exit plan.\n" + json.dumps(
                    job
                )
                headers = {"Authorization": "Bearer " + create_jwt(owner, role="user")}
                async with session.post(
                    f"http://127.0.0.1:{WEB_PORT}/api/v1/agents/telegram_agent/ask",
                    json={"task": prompt, "server_name": "main", "caller": "telegram-exit-planner"},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=90),
                ) as response:
                    response.raise_for_status()
                    answer = await response.json()
                plan = json.loads(answer["answer"])
                async with session.post(
                    trader_url + "/plans",
                    headers=trader_headers,
                    json={
                        "trade_id": job["trade_id"],
                        "version": job["version"],
                        "lease": job["lease"],
                        "plan": plan,
                    },
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    response.raise_for_status()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 -- model job boundary, never retry exchange writes
                # No exception payloads: URLs can contain credentials. The service
                # retains job state; model failure never cancels/closes a position.
                log.warning("Telegram exit planner: %s", type(exc).__name__)
                if job:
                    try:
                        async with session.post(
                            trader_url + "/jobs/fail",
                            headers=trader_headers,
                            json={
                                "trade_id": job["trade_id"],
                                "lease": job["lease"],
                                "error": type(exc).__name__,
                            },
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as response:
                            response.raise_for_status()
                    except (aiohttp.ClientError, TimeoutError):
                        log.warning("Exit planning failure notification unavailable")
                await asyncio.sleep(5)
