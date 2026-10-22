"""Read authoritative channel execution status; optionally pause NEW entries."""

import json
import os
from typing import Literal

import aiohttp
from pydantic import BaseModel
from routines.base import RoutineResult


class Config(BaseModel):
    action: Literal["status", "pause"] = "status"


async def run(config: Config, context):
    url = os.environ["TELEGRAM_TRADER_URL"].rstrip("/")
    headers = {"Authorization": "Bearer " + os.environ["TRADER_TOKEN"]}
    async with aiohttp.ClientSession(headers=headers) as session:
        method = "GET" if config.action == "status" else "POST"
        async with session.request(
            method, url + "/" + config.action, timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            response.raise_for_status()
            result = await response.json()
    return RoutineResult(text=json.dumps(result, ensure_ascii=False, indent=2))
