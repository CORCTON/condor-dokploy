"""Fault-injected local exchange simulations; never connects to an exchange."""

import copy
import time

import pytest
from test_trader import Exchange, plan, signal

from trader.engine import Engine, Review
from trader.gate import GateError
from trader.models import Plan
from trader.store import Store


@pytest.fixture
def rig(tmp_path):
    store = Store(tmp_path / "execution.sqlite3")
    exchange = Exchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield store, exchange, engine
    store.close()


async def planned_trade(rig):
    trade = await signal(rig)
    job = await rig[2].claim_plan()
    await rig[2].propose_plan(job["trade_id"], job["version"], job["lease"], plan())
    await rig[2].apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    return rig[0].trade(trade["id"])


@pytest.mark.parametrize("expired", [False, True])
async def test_stop_replacement_rejection_keeps_receipt_and_recovers(rig, monkeypatch, expired):
    store, exchange, engine = rig
    trade = await planned_trade(rig)
    old_id = trade["exits"]["sl"]["id"]
    trade["exits"]["sl"]["created"] = time.time() - 83000
    store.save_trade(trade)
    if expired:
        exchange.stops[old_id].update(status="finished", finish_as="expired")
    request = exchange.request

    async def reject(method, path, body=None, params=None):
        if method == "POST" and path == "/price_orders":
            raise GateError(400, "INJECTED_REJECTION")
        return await request(method, path, body, params)

    monkeypatch.setattr(exchange, "request", reject)
    await engine.poll()
    failed = store.trade(trade["id"])
    assert failed["exits"]["sl"]["id"] == old_id
    assert not failed["retired_stops"]
    assert failed["last_error"] == "Gate 400: INJECTED_REJECTION"
    assert len(exchange.stops) == 1

    monkeypatch.setattr(exchange, "request", request)
    await engine.poll()
    recovered = store.trade(trade["id"])
    assert recovered["exits"]["sl"]["id"] != old_id
    assert exchange.stops[recovered["exits"]["sl"]["id"]]["status"] == "open"
    assert exchange.stops[old_id]["status"] == "finished"
    assert not recovered.get("last_error")
    assert not recovered["retired_stops"]
    assert not store.unknowns()


@pytest.mark.parametrize("visible", [False, True])
async def test_unknown_stop_replacement_never_resends_or_hides_error(rig, monkeypatch, visible):
    store, exchange, engine = rig
    trade = await planned_trade(rig)
    old_id = trade["exits"]["sl"]["id"]
    trade["exits"]["sl"]["created"] = time.time() - 83000
    store.save_trade(trade)
    request = exchange.request

    async def lose_receipt(method, path, body=None, params=None):
        result = await request(method, path, body, params)
        if method == "POST" and path == "/price_orders":
            raise TimeoutError("injected after stop creation")
        return result

    monkeypatch.setattr(exchange, "request", lose_receipt)
    await engine.poll()
    monkeypatch.setattr(exchange, "request", request)
    get = exchange.get

    async def lookup(path, **params):
        if path == "/price_orders" and not visible:
            return []
        return await get(path, **params)

    monkeypatch.setattr(exchange, "get", lookup)
    for _ in range(3):
        await engine.poll()
    current = store.trade(trade["id"])
    assert len(exchange.stops) == 2  # the unacknowledged stop was not duplicated
    assert engine.paused()
    if visible:
        assert current["exits"]["sl"]["id"] != old_id
        assert exchange.stops[old_id]["status"] == "finished"
        assert not store.unknowns()
        assert not current.get("last_error")
    else:
        assert current["exits"]["sl"]["id"] == old_id
        assert exchange.stops[old_id]["status"] == "open"
        assert len(store.unknowns()) == 1
        assert current["last_error"]


async def test_stop_replacement_recovers_confirmed_receipt_after_crash(rig, monkeypatch):
    store, exchange, engine = rig
    trade = await planned_trade(rig)
    old_id = trade["exits"]["sl"]["id"]
    save = store.save_trade

    def crash_before_trade_commit(value):
        if value["exits"].get("sl", {}).get("id") != old_id:
            raise RuntimeError("injected crash after durable exchange receipt")
        save(value)

    monkeypatch.setattr(store, "save_trade", crash_before_trade_commit)
    with pytest.raises(RuntimeError, match="durable exchange receipt"):
        await engine.renew_stop(trade)
    monkeypatch.setattr(store, "save_trade", save)
    assert store.trade(trade["id"])["exits"]["sl"]["id"] == old_id
    assert len(exchange.stops) == 2
    restarted = Engine(store, exchange, enabled=True)
    await restarted.poll()
    recovered = store.trade(trade["id"])
    assert recovered["exits"]["sl"]["id"] != old_id
    assert exchange.stops[old_id]["status"] == "finished"
    assert len(exchange.stops) == 2
    assert not recovered["retired_stops"]
    assert not recovered.get("last_error")


@pytest.mark.parametrize("combined", [False, True])
async def test_limit_entry_retains_channel_levels_until_actual_fill(rig, combined):
    store, exchange, engine = rig
    trade = await signal(rig, "#INJ 在10限价入场空单")
    text = "INJ SL: 10.50 TP1: 9.50 TP2: 8.50" if combined else "INJ SL: 10.50"
    await signal(rig, text, message=2)
    pending = store.trade(trade["id"])
    assert pending["channel_levels"]["sl"] == "10.50"
    assert not pending["source_sl"]
    assert not exchange.stops
    assert store.db.execute("SELECT status FROM events WHERE message=2").fetchone()[0] == "done"
    if combined:
        assert pending["channel_levels"]["tp1"] == "9.50"
    exchange.orders[trade["entry_id"]].update(left=0, status="finished")
    exchange.position["size"] = -trade["lots"]
    await engine.poll()
    filled = store.trade(trade["id"])
    assert filled["source_sl"]
    assert filled["exits"]["sl"]["price"] == "10.50"
    job = await engine.claim_plan()
    assert job["channel_levels"]["sl"] == "10.50"
    await engine.propose_plan(job["trade_id"], job["version"], job["lease"], plan())
    await engine.apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    if combined:
        assert exchange.orders["2"]["price"] == "9.50"
        assert exchange.orders["3"]["price"] == "8.50"


async def test_pending_stop_survives_order_position_visibility_lag(rig):
    store, exchange, engine = rig
    trade = await signal(rig, "#INJ 在10限价入场空单")
    await signal(rig, "INJ SL: 10.50", message=2)
    exchange.orders[trade["entry_id"]].update(left=0, status="finished")
    await engine.poll()  # fill is known, position API has not caught up yet
    assert not exchange.stops
    exchange.position["size"] = -trade["lots"]
    await engine.poll()
    assert store.trade(trade["id"])["exits"]["sl"]["price"] == "10.50"


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("at_entry", [False, True])
@pytest.mark.parametrize("combined", [False, True])
async def test_confirmed_channel_profit_stop_does_not_block_first_tp_plan(
    rig, side, at_entry, combined
):
    store, exchange, engine = rig
    text = "#INJ 当前价格入场一笔多单" if side == "long" else "#INJ 当前价格入场一笔空单"
    trade = await signal(rig, text)
    exchange.position["mark_price"] = "11.50" if side == "long" else "8.50"
    sl = "10" if at_entry else ("10.50" if side == "long" else "9.50")
    tp1, tp2 = ("12", "13") if side == "long" else ("8", "7")
    levels = f"INJ SL: {sl}" + (f" TP1: {tp1} TP2: {tp2}" if combined else "")
    await signal(rig, levels, message=2)
    job = await engine.claim_plan()
    proposal = Plan(
        sl="9" if side == "long" else "11",
        tp1=tp1,
        tp2=tp2,
        tp1_fraction="0.5",
        rationale="isolated simulation",
    )
    await engine.propose_plan(job["trade_id"], job["version"], job["lease"], proposal)
    await engine.apply_plan(job["trade_id"], job["version"], job["lease"], proposal)
    current = store.trade(trade["id"])
    assert set(current["exits"]) == {"sl", "tp1", "tp2"}
    assert current["exits"]["sl"]["price"] == sl
    assert not current["plan_needed"]
    assert len(exchange.stops) == 1


async def test_profit_stop_relaxation_requires_still_active_exchange_stop(rig):
    store, exchange, engine = rig
    trade = await signal(rig)
    exchange.position["mark_price"] = "9"
    await signal(rig, "INJ SL: 9.50", message=2)
    job = await engine.claim_plan()
    exchange.stops[store.trade(trade["id"])["exits"]["sl"]["id"]].update(
        status="finished", finish_as="expired"
    )
    with pytest.raises(Review, match="no longer active"):
        await engine.propose_plan(job["trade_id"], job["version"], job["lease"], plan())
        await engine.apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    assert len(exchange.orders) == 1


@pytest.mark.parametrize("fill_during_cancel", [False, True])
async def test_market_tp_cancels_unfilled_entry_and_reconciles_cancel_race(
    rig, monkeypatch, fill_during_cancel
):
    store, exchange, engine = rig
    trade = await signal(rig, "#INJ 在10限价入场空单")
    entry = exchange.orders[trade["entry_id"]]
    entry["left"] = trade["lots"] - 4
    exchange.position["size"] = -4
    await engine.poll()
    job = await engine.claim_plan()
    await engine.propose_plan(job["trade_id"], job["version"], job["lease"], plan())
    await engine.apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    request = exchange.request

    async def race(method, path, body=None, params=None):
        if fill_during_cancel and method == "DELETE" and path == f"/orders/{entry['id']}":
            exchange.position["size"] -= abs(entry["left"])
            entry["left"] = 0
        return await request(method, path, body, params)

    monkeypatch.setattr(exchange, "request", race)
    before = len(exchange.writes)
    await signal(rig, "INJ 当前价格做TP1", message=2)
    for _ in range(2):
        await engine.poll()
    current = store.trade(trade["id"])
    expected_remaining = 7 if fill_during_cancel else 2
    assert entry["status"] == "finished"
    assert current["entry_terminal"]
    assert exchange.position["size"] == -expected_remaining
    assert current["exits"]["tp2"]["size"] == expected_remaining
    assert exchange.writes[before][:2] == ("DELETE", f"/orders/{entry['id']}")
    assert not [
        o for o in exchange.orders.values() if not o["reduce_only"] and o["status"] == "open"
    ]


async def test_market_tp_does_not_reduce_until_entry_cancellation_is_confirmed(rig, monkeypatch):
    _store, exchange, engine = rig
    trade = await signal(rig, "#INJ 在10限价入场空单")
    entry = exchange.orders[trade["entry_id"]]
    entry["left"] = trade["lots"] - 4
    exchange.position["size"] = -4
    await engine.poll()
    job = await engine.claim_plan()
    await engine.propose_plan(job["trade_id"], job["version"], job["lease"], plan())
    await engine.apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    request = exchange.request

    async def lose_cancel(method, path, body=None, params=None):
        result = await request(method, path, body, params)
        if method == "DELETE" and path == f"/orders/{entry['id']}":
            raise TimeoutError("injected lost cancel acknowledgement")
        return result

    monkeypatch.setattr(exchange, "request", lose_cancel)
    await signal(rig, "INJ 当前价格做TP1", message=2)
    assert exchange.position["size"] == -4
    assert not [o for o in exchange.orders.values() if o["reduce_only"] and o["price"] == "0"]
    assert engine.paused()


async def test_exit_plan_requires_an_immutable_accepted_proposal(rig):
    _store, exchange, engine = rig
    await signal(rig)
    job = await engine.claim_plan()
    args = job["trade_id"], job["version"], job["lease"]
    before = len(exchange.writes)
    with pytest.raises(Review, match="accepted exit-plan proposal"):
        await engine.apply_plan(*args, plan())
    await engine.propose_plan(*args, plan())
    await engine.propose_plan(*args, plan())  # identical delivery is idempotent
    changed = Plan(sl="11", tp1="9.50", tp2="8", tp1_fraction="0.5", rationale="changed")
    with pytest.raises(Review, match="accepted exit plan cannot be changed"):
        await engine.propose_plan(*args, changed)
    with pytest.raises(Review, match="accepted exit-plan proposal"):
        await engine.apply_plan(*args, changed)
    assert len(exchange.writes) == before
    await engine.apply_plan(*args, plan())
    assert len(exchange.stops) == 1
    assert len(exchange.orders) == 3


@pytest.mark.parametrize("status", ["open", "finished"])
@pytest.mark.parametrize("response_aliases", [False, True])
async def test_stop_receipt_search_reads_all_pages_and_both_statuses(
    rig, monkeypatch, status, response_aliases
):
    _store, exchange, engine = rig
    await planned_trade(rig)
    receipt = copy.deepcopy(exchange.stops["100"])
    receipt["status"] = status
    body = {key: copy.deepcopy(receipt[key]) for key in ("initial", "trigger", "order_type")}
    if response_aliases:
        receipt["initial"]["is_close"] = receipt["initial"].pop("close")
        receipt["initial"]["is_reduce_only"] = receipt["initial"].pop("reduce_only")
        receipt["initial"].update(iceberg=0, auto_size="", amount="0", iceberg_amount="0")
        receipt["trigger"]["bbo"] = ""
    unrelated = copy.deepcopy(receipt)
    unrelated["initial"]["text"] = "t-unrelated"
    calls = []

    async def pages(path, **params):
        assert path == "/price_orders" and params["contract"] == "INJ_USDT"
        calls.append((params["status"], params["offset"]))
        if params["status"] != status:
            return []
        return [unrelated] * 100 if params["offset"] == 0 else [receipt]

    monkeypatch.setattr(exchange, "get", pages)
    assert await engine.find_stop_receipt(body) == receipt
    assert (status, 0) in calls and (status, 100) in calls
    assert {item[0] for item in calls} == {"open", "finished"}


@pytest.mark.parametrize(
    "change",
    ["contract", "price", "rule", "close", "reduce_only", "size", "order_type", "duplicate"],
)
async def test_stop_receipt_search_rejects_tag_only_or_ambiguous_matches(rig, monkeypatch, change):
    _store, exchange, engine = rig
    await planned_trade(rig)
    receipt = copy.deepcopy(exchange.stops["100"])
    body = {key: copy.deepcopy(receipt[key]) for key in ("initial", "trigger", "order_type")}
    if change == "contract":
        receipt["initial"]["contract"] = "BTC_USDT"
    elif change in {"price", "rule"}:
        receipt["trigger"][change] = "12" if change == "price" else 2
    elif change in {"close", "reduce_only", "size"}:
        receipt["initial"][change] = 1 if change == "size" else False
    elif change == "order_type":
        receipt["order_type"] = "plan-close-long-position"
    duplicate = copy.deepcopy(receipt)
    duplicate["id"] = "999"

    async def lookup(path, **params):
        assert path == "/price_orders"
        if params["status"] == "finished":
            return []
        return [receipt, duplicate] if change == "duplicate" else [receipt]

    monkeypatch.setattr(exchange, "get", lookup)
    assert await engine.find_stop_receipt(body) is None


async def test_stop_receipt_search_does_not_confirm_an_incomplete_page_scan(rig, monkeypatch):
    store, exchange, engine = rig
    trade = await planned_trade(rig)
    receipt = copy.deepcopy(exchange.stops["100"])
    body = {key: copy.deepcopy(receipt[key]) for key in ("initial", "trigger", "order_type")}
    store.intent("unknown-stop", trade["id"], "POST", "/price_orders", {"body": body})
    unrelated = copy.deepcopy(receipt)
    unrelated["initial"]["text"] = "t-unrelated"

    async def pages(path, **params):
        assert path == "/price_orders"
        if params["offset"]:
            raise GateError(503, "INJECTED_PAGE_FAILURE")
        return [receipt] + [unrelated] * 99

    monkeypatch.setattr(exchange, "get", pages)
    await engine.reconcile_unknowns()
    assert store.operation("unknown-stop")["status"] == "unknown"
    assert engine.paused()


async def test_legacy_partial_tp_state_requires_reconciliation_not_guessed_reduction(rig):
    _store, exchange, engine = rig
    trade = await planned_trade(rig)
    item = trade["exits"]["tp1"]
    item.update(take_attempt=1, take_pending=True)
    assert "take_filled" not in item
    before = len(exchange.writes)
    with pytest.raises(Review, match="legacy TP reduction"):
        await engine.take_now(trade, "tp1")
    with pytest.raises(Review, match="legacy TP reduction"):
        await engine.resize_targets(trade)
    assert len(exchange.writes) == before
