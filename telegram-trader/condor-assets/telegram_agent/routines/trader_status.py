"""查询跟单状态，或按管理员指令开始／暂停新开仓；已有仓位继续管理。"""

import json
import os
from typing import Literal

import aiohttp
from pydantic import BaseModel
from routines.base import RoutineResult


class Config(BaseModel):
    action: Literal["status", "start", "pause"] = "status"


async def run(config: Config, context):
    if getattr(context, "_chat_id", None) != int(os.environ["ADMIN_USER_ID"]):
        raise PermissionError("只有已配置的 Telegram 管理员可以查询和控制跟单。")
    url = os.environ["TELEGRAM_TRADER_URL"].rstrip("/")
    headers = {"Authorization": "Bearer " + os.environ["TRADER_TOKEN"]}
    async with aiohttp.ClientSession(headers=headers) as session:
        method = "GET" if config.action == "status" else "POST"
        async with session.request(
            method, url + "/" + config.action, timeout=aiohttp.ClientTimeout(total=20)
        ) as response:
            if response.status >= 400:
                return RoutineResult(
                    text=json.dumps(
                        {
                            "ok": False,
                            "action": config.action,
                            "reason": await response.text(),
                        },
                        ensure_ascii=False,
                    )
                )
            result = await response.json()
    return RoutineResult(
        text=json.dumps(
            {"ok": True, "action": config.action, **result}, ensure_ascii=False, indent=2
        )
    )
