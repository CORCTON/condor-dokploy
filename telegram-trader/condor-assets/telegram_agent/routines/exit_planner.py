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


async def generate_plan(job, owner):
    from condor.acp.client import PromptDone, TextChunk
    from condor.acp.pydantic_ai_client import is_pydantic_ai_model
    from condor.agents.agent import AgentStore
    from condor.runtime import context as runtime_context
    from condor.runtime.llm_client import build_llm_client

    agent = AgentStore().get("telegram_agent")
    if agent is None or not is_pydantic_ai_model(agent.agent_key):
        raise ValueError("Exit planner requires a configured API model, not a shell/ACP agent")
    # Use Condor's provider resolution and Agent context, but attach NO MCP
    # servers. Untrusted channel text cannot induce a model-side tool action.
    client = build_llm_client(agent.agent_key, mcp_servers=[], user_id=owner)
    task = "EXIT_PLAN_JOB\nReturn only the prescribed JSON exit plan.\n" + json.dumps(job)
    prompt = runtime_context.build_agent_context(agent, owner, task, "")
    chunks, reason = [], ""
    try:
        await client.start()
        async for event in client.prompt_stream(prompt):
            if isinstance(event, TextChunk):
                chunks.append(event.text)
            elif isinstance(event, PromptDone):
                reason = event.stop_reason
    finally:
        await client.stop()
    if reason != "end_turn":
        raise RuntimeError("Exit planner did not complete normally")
    return json.loads("".join(chunks))


async def run(config: Config, context):

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
                plan = await asyncio.wait_for(generate_plan(job, owner), timeout=90)
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
