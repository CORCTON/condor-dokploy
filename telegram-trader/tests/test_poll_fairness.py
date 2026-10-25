"""Backlog/lock scheduling regressions; only the local exchange simulator is used."""

import asyncio
import time

import pytest
from test_execution_reliability import planned_trade
from test_trader import Exchange

from trader.engine import Engine, Review
from trader.models import Signal
from trader.signals import SignalInbox
from trader.store import Store


@pytest.fixture
def rig(tmp_path):
    store = Store(tmp_path / "fairness.sqlite3")
    exchange = Exchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield store, exchange, engine
    store.close()


def queue_message(store):
    key = store.event(-100123, 20, "频道讨论，模型尚未处理", None, time.time(), True)
    return store.db.execute("SELECT * FROM events WHERE id=?", (key,)).fetchone()


def seed_trades(store):
    for name in ("first", "second", "third"):
        store.save_trade({"id": name, "symbol": name, "status": "working", "exits": {}})


async def test_unprocessed_channel_backlog_does_not_block_stop_renewal(rig):
    store, exchange, engine = rig
    trade = await planned_trade(rig)
    old_stop = trade["exits"]["sl"]["id"]
    trade["exits"]["sl"]["created"] = time.time() - 83000
    store.save_trade(trade)
    queue_message(store)

    await engine.poll()

    current = store.trade(trade["id"])
    assert current["exits"]["sl"]["id"] != old_stop
    assert exchange.stops[current["exits"]["sl"]["id"]]["status"] == "open"
    assert exchange.stops[old_stop]["status"] == "finished"
    assert len(store.queued()) == 1  # maintenance did not consume/reinterpret the message
    assert not engine.paused()
    assert not store.unknowns()


@pytest.mark.parametrize("first_fails", [False, True])
async def test_backlog_cannot_starve_any_position(rig, monkeypatch, first_fails):
    store, _, engine = rig
    seed_trades(store)
    queue_message(store)
    visited = []

    async def refresh(trade):
        assert engine.lock.locked()
        visited.append(trade["id"])
        if first_fails and trade["id"] == "first":
            raise Review("simulated position needs review")

    monkeypatch.setattr(engine, "recover_exits", lambda trade: None)
    monkeypatch.setattr(engine, "refresh", refresh)
    for _ in range(3):
        visited.clear()
        await engine.poll()
        assert sorted(visited) == ["first", "second", "third"]
    assert len(store.queued()) == 1
    assert engine.paused() == first_fails


async def test_ready_channel_action_runs_between_positions(rig, monkeypatch):
    store, _, engine = rig
    seed_trades(store)
    event = queue_message(store)
    started, release = asyncio.Event(), asyncio.Event()
    visited = []

    async def refresh(trade):
        visited.append(trade["id"])
        if trade["id"] == "first":
            started.set()
            await release.wait()

    execute = engine.execute_decision

    async def decision(*args):
        visited.append("channel")
        await execute(*args)

    monkeypatch.setattr(engine, "recover_exits", lambda trade: None)
    monkeypatch.setattr(engine, "refresh", refresh)
    monkeypatch.setattr(engine, "execute_decision", decision)
    polling = asyncio.create_task(engine.poll())
    processing = None
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        processing = asyncio.create_task(engine.process(event, Signal(action="ignore")))
        await asyncio.sleep(0)  # the ready channel operation waits for the current position
        release.set()
        await asyncio.wait_for(asyncio.gather(polling, processing), timeout=1)
    finally:
        for task in (polling, processing):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*(t for t in (polling, processing) if t), return_exceptions=True)
    assert visited == ["first", "channel", "second", "third"]


async def test_position_closed_while_waiting_for_lock_is_not_revisited(rig, monkeypatch):
    store, _, engine = rig
    seed_trades(store)
    visited = []

    async def refresh(trade):
        visited.append(trade["id"])
        if trade["id"] == "first":
            closed = store.trade("second")
            closed["status"] = "closed"
            store.save_trade(closed)

    monkeypatch.setattr(engine, "recover_exits", lambda trade: None)
    monkeypatch.setattr(engine, "refresh", refresh)
    await engine.poll()
    assert visited == ["first", "third"]


async def test_poll_yields_to_newly_scheduled_channel_action(rig, monkeypatch):
    store, _, engine = rig
    seed_trades(store)
    event = queue_message(store)
    visited, processing = [], []
    execute = engine.execute_decision

    async def decision(*args):
        visited.append("channel")
        await execute(*args)

    async def unknowns():
        visited.append("unknowns")

    async def refresh(trade):
        visited.append(trade["id"])
        if trade["id"] == "first":
            processing.append(asyncio.create_task(engine.process(event, Signal(action="ignore"))))
        # No await here: cached/immediate I/O must not make the poll monopolize the loop.

    monkeypatch.setattr(engine, "reconcile_unknowns", unknowns)
    monkeypatch.setattr(engine, "recover_exits", lambda trade: None)
    monkeypatch.setattr(engine, "refresh", refresh)
    monkeypatch.setattr(engine, "execute_decision", decision)
    try:
        await engine.poll()
        await asyncio.gather(*processing)
    finally:
        for task in processing:
            if not task.done():
                task.cancel()
        await asyncio.gather(*processing, return_exceptions=True)
    assert visited == ["unknowns", "first", "channel", "second", "third"]


async def test_reconciliation_row_reordering_does_not_stale_model_lease(rig):
    store, _, engine = rig
    seed_trades(store)
    queue_message(store)
    inbox = SignalInbox(engine)
    job = await inbox.claim()
    assert inbox.fresh(job["lease"])

    store.save_trade(store.trade("first"))  # same data, new SQLite rowid during a poll
    assert [trade["id"] for trade in engine.active()] == ["second", "third", "first"]
    assert inbox.fresh(job["lease"])


@pytest.mark.parametrize("field,value", [
    ("version", 1), ("status", "closed"), ("filled", 1),
    ("position_size", 1), ("entry_terminal", True),
])
async def test_real_position_change_still_stales_model_lease(rig, field, value):
    store, _, engine = rig
    seed_trades(store)
    queue_message(store)
    inbox = SignalInbox(engine)
    job = await inbox.claim()
    trade = store.trade("first")
    trade[field] = value
    store.save_trade(trade)
    assert not inbox.fresh(job["lease"])
