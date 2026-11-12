"""Run in the installed Hummingbot API image, --network none, no account mounts.

Uses the real API router, validation, service create/stop and PositionExecutor.
Only exchange I/O and scheduling are simulated: deterministic ticks replace the
background clock. This is not a Gate testnet fill or restart-recovery test.
"""

import argparse
import asyncio
import copy
import hashlib
import inspect
import json
import time
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PriceType
from hummingbot.strategy_v2.executors.position_executor.position_executor import (
    PositionExecutor,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from proposal import Proposal, position_config
from routers.executors import get_executor_service, router
from services.executor_service import ExecutorService


def make_runtime(config):
    pair = config["trading_pair"]
    mid = D(config["entry_price"])
    connector = Mock()
    connector.trading_rules = {pair: TradingRule(pair, min_order_size=D("0.0001"),
                                                min_price_increment=D("0.0001"),
                                                min_base_amount_increment=D("0.0001"))}
    connector.get_price_by_type.return_value = mid
    strategy = Mock()
    strategy.current_timestamp = time.time()
    strategy.connectors = {"gate_io_perpetual": connector}
    strategy.buy.return_value = "simulated-buy-order"
    strategy.sell.return_value = "simulated-sell-order"
    service = ExecutorService(Mock(), None)
    service._get_trading_interface = Mock(return_value=strategy)
    service._prepare_market = AsyncMock()
    service._persist_executor_created = AsyncMock()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_executor_service] = lambda: service

    def start(executor):
        executor._status = RunnableStatus.RUNNING

    with patch.object(PositionExecutor, "start", start), TestClient(app) as client:
        response = client.post("/executors/", json={"executor_config": config,
                              "account_name": "isolated_rehearsal", "controller_id": "activation-eval"})
        assert response.status_code == 201, response.text
    executor = service._active_executors[response.json()["executor_id"]]
    return SimpleNamespace(executor=executor, connector=connector, strategy=strategy,
                           service=service, app=app)


def step(runtime, price):
    runtime.connector.get_price_by_type.return_value = D(str(price))
    asyncio.run(runtime.executor.control_task())
    return runtime.strategy.buy.call_count + runtime.strategy.sell.call_count


def check_config(config, venue_rules=None, proposal=None):
    centre = D(config["entry_price"])
    a, b = map(D, config["activation_bounds"])
    lower, upper = (centre * (1 - a), centre * (1 + b)) if config["side"] == 1 else (
        centre * (1 - b), centre * (1 + a))
    epsilon = (upper - lower) / 10
    before, beyond = (lower - epsilon, upper + epsilon) if config["side"] == 1 else (
        upper + epsilon, lower - epsilon)
    checks = []
    if proposal:
        assert config["trading_pair"] == proposal.trading_pair
        assert config["side"] == (1 if proposal.side == "long" else 2)
        tolerance = max(proposal.upper, D(1)) * D("1e-20")
        assert abs(lower - proposal.lower) < tolerance
        assert abs(upper - proposal.upper) < tolerance
        checks.append("model_proposal_bounds_preserved")
    if venue_rules:
        amount = D(config["amount"])
        assert amount % D(str(venue_rules["min_base_amount_increment"])) == 0
        assert amount >= D(str(venue_rules["min_order_size"]))
        assert amount * lower >= D(str(venue_rules["min_notional_size"]))
        checks.append("venue_lot_minimum_and_notional")
    rt = make_runtime(copy.deepcopy(config))
    assert step(rt, before) == 0
    checks.append("wait_outside")
    assert step(rt, centre) == 1
    assert rt.connector.get_price_by_type.call_args.args[-1] == PriceType.MidPrice
    call = rt.strategy.buy.call_args if config["side"] == 1 else rt.strategy.sell.call_args
    assert call.args[3] == OrderType.MARKET
    checks.append("midprice_enters_submits_market")
    assert step(rt, centre) == 1
    checks.append("repeated_ticks_no_duplicate")
    for edge in (lower, upper):
        assert step(make_runtime(copy.deepcopy(config)), edge) == 1
    checks.append("inclusive_boundaries")
    rt = make_runtime(copy.deepcopy(config))
    assert step(rt, before) == 0 and step(rt, beyond) == 0
    checks.append("gap_over_range_no_order")
    assert step(rt, centre) == 1
    checks.append("return_after_gap_still_triggers_native_limitation")
    assert step(make_runtime(copy.deepcopy(config)), centre) == 1
    checks.append("already_inside_triggers_native_limitation")
    rt = make_runtime(copy.deepcopy(config))
    assert step(rt, before) == 0
    rt.executor._sleep = AsyncMock()
    with TestClient(rt.app) as client:
        response = client.post(f"/executors/{rt.executor.config.id}/stop",
                               json={"keep_position": True})
        assert response.status_code == 200, response.text
    assert step(rt, centre) == 0
    assert rt.executor.status == RunnableStatus.TERMINATED
    checks.append("cancel_pending_prevents_entry")
    return checks


def main(args):
    reports = []
    for side, lower, upper in (("long", "10.5", "10.6"), ("short", "9.4", "9.5")):
        proposal = Proposal(action="arm", trading_pair="INJ-USDT", side=side,
                            lower=lower, upper=upper, evidence_message_ids=[101],
                            structural_lower="10.49" if side == "long" else "9.51",
                            structural_upper="10.49" if side == "long" else "9.51",
                            rationale="Deterministic API and executor boundary verification")
        config = position_config(proposal, mid=D(10), tick=D("0.01"), amount=D(1), leverage=10)
        reports.append({"case": side, "checks": check_config(config)})
    if args.proposals:
        for line in args.proposals.read_text().splitlines():
            result = json.loads(line)
            if result.get("executor_config"):
                proposal = Proposal.model_validate(result["proposal"])
                reports.append({"case": result["case"], "repeat": result["repeat"],
                                "checks": check_config(result["executor_config"], result.get("venue_rules"), proposal)})
    source = Path(inspect.getfile(PositionExecutor))
    result = {"passed": True, "official_source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
              "reports": reports, "real_exchange_writes": 0,
              "scope": "real API/router/service/executor, simulated connector and deterministic ticks"}
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proposals", type=Path)
    main(parser.parse_args())
