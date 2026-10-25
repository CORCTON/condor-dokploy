"""Execution/protocol tests with scripted model decisions, NOT model accuracy claims."""

import time

import pytest
from test_trader import Exchange

from trader.engine import Engine, Review
from trader.models import Decisions
from trader.signals import SignalInbox
from trader.store import Store


@pytest.fixture
def inbox(tmp_path):
    store = Store(tmp_path / "signals.sqlite3")
    engine = Engine(store, Exchange(), enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    value = SignalInbox(engine)
    yield value
    store.close()


def receive(inbox, text, message=1, reply=None, live=True, media=None):
    return inbox.store.event(-100123, message, text, reply, time.time(), live, media=media)


def answer(job, action="open", **fields):
    return Decisions.model_validate(
        {
            "decisions": [
                dict(
                    event_id=event["event_id"],
                    action=action,
                    reason="scripted model decision",
                    evidence_message_ids=[event["message_id"]],
                    **fields,
                )
                for event in job["events"]
            ]
        }
    )


async def apply(inbox, lease, decisions):
    if inbox.store.analysis(lease)["status"] != "done":
        await inbox.propose(lease, decisions)
    await inbox.apply(lease, decisions)


async def open_trade(inbox, price=None):
    receive(inbox, "#INJ 开一笔空单")
    job = await inbox.claim()
    await apply(inbox, job["lease"], answer(job, symbol="INJ", side="short", price=price))
    return inbox.store.trades()[0]


async def test_every_message_including_chatter_and_images_goes_to_model(inbox):
    receive(inbox, "早上好")
    receive(inbox, "", message=2, media={"id": "photo-2", "mime": "image/jpeg"})
    job = await inbox.claim()
    assert [e["text"] for e in job["events"]] == ["早上好", ""]
    assert job["events"][1]["media"]["id"] == "photo-2"
    assert not inbox.engine.gate.writes
    await apply(inbox, job["lease"], answer(job, "ignore"))
    assert not inbox.engine.gate.writes


@pytest.mark.parametrize("text", ["平掉", "离场", "撤掉，仓位也不要留了"])
async def test_model_associated_exit_closes_owned_trade(inbox, text):
    trade = await open_trade(inbox)
    receive(inbox, text, message=2, reply=1)
    job = await inbox.claim()
    assert job["events"][0]["reply_to_message_id"] == 1
    assert job["trades"][0]["id"] == trade["id"]
    await apply(inbox, job["lease"], answer(job, "close", target_trade_id=trade["id"]))
    assert inbox.engine.gate.position["size"] == 0


async def test_cancel_pending_entry_is_not_a_market_close(inbox):
    trade = await open_trade(inbox, price="10")
    receive(inbox, "撤掉", message=2, reply=1)
    job = await inbox.claim()
    await apply(inbox, job["lease"], answer(job, "cancel", target_trade_id=trade["id"]))
    assert inbox.engine.gate.orders[trade["entry_id"]]["status"] == "finished"
    assert len(inbox.engine.gate.orders) == 1
    assert inbox.engine.gate.position["size"] == 0


async def test_cancel_does_not_close_already_filled_position(inbox):
    trade = await open_trade(inbox)
    receive(inbox, "撤掉挂单", message=2)
    job = await inbox.claim()
    await apply(inbox, job["lease"], answer(job, "cancel", target_trade_id=trade["id"]))
    assert inbox.engine.gate.position["size"] == -14
    assert len(inbox.engine.gate.orders) == 1


async def test_new_message_invalidates_old_analysis_before_entry(inbox):
    receive(inbox, "INJ 开空")
    old = await inbox.claim()
    receive(inbox, "算了撤掉", message=2, reply=1)
    assert not inbox.fresh(old["lease"])
    with pytest.raises(Review, match="stale"):
        await apply(inbox, old["lease"], answer(old, symbol="INJ", side="short"))
    job = await inbox.claim()
    assert len(job["events"]) == 2
    await apply(inbox, job["lease"], answer(job, "ignore"))
    assert not inbox.engine.gate.writes


async def test_replay_of_accepted_model_response_does_not_trade_twice(inbox):
    receive(inbox, "INJ 开空")
    job = await inbox.claim()
    decisions = answer(job, symbol="INJ", side="short")
    await apply(inbox, job["lease"], decisions)
    before = len(inbox.engine.gate.writes)
    await apply(inbox, job["lease"], decisions)
    assert len(inbox.engine.gate.writes) == before


async def test_late_failure_notification_cannot_reopen_completed_analysis(inbox):
    receive(inbox, "早上好")
    job = await inbox.claim()
    decisions = answer(job, "ignore")
    await apply(inbox, job["lease"], decisions)
    inbox.failed(job["lease"], "TimeoutError")
    assert inbox.store.analysis(job["lease"])["status"] == "done"
    await apply(inbox, job["lease"], decisions)
    assert not inbox.store.queued() and not inbox.engine.gate.writes


async def test_proposal_never_executes_and_must_remain_fresh(inbox):
    receive(inbox, "INJ 开空")
    job = await inbox.claim()
    decisions = answer(job, symbol="INJ", side="short")
    await inbox.propose(job["lease"], decisions)
    assert inbox.proposal(job["lease"]) == decisions.model_dump()
    assert not inbox.engine.gate.writes
    receive(inbox, "取消", message=2)
    with pytest.raises(Review, match="stale"):
        inbox.proposal(job["lease"])
    with pytest.raises(Review, match="stale"):
        await inbox.propose(job["lease"], decisions)
    assert not inbox.engine.gate.writes


async def test_model_cannot_submit_arbitrary_history_as_current_event(inbox):
    receive(inbox, "现在取消之前的计划")
    job = await inbox.claim()
    decisions = answer(job, symbol="INJ", side="short")
    decisions.decisions[0].event_id = "historical-message"
    with pytest.raises(Review, match="exactly"):
        await apply(inbox, job["lease"], decisions)
    assert not inbox.engine.gate.writes


async def test_incomplete_batch_rejected_before_any_execution(inbox):
    receive(inbox, "INJ 开空")
    receive(inbox, "别开了", message=2)
    job = await inbox.claim()
    decisions = answer(job, symbol="INJ", side="short")
    decisions.decisions.pop()
    with pytest.raises(Review, match="exactly"):
        await apply(inbox, job["lease"], decisions)
    assert not inbox.engine.gate.writes


async def test_model_must_name_owned_trade_not_just_coin(inbox):
    await open_trade(inbox)
    receive(inbox, "平掉", message=2)
    job = await inbox.claim()
    with pytest.raises(Review, match="owned trade"):
        await apply(inbox, job["lease"], answer(job, "close", symbol="INJ"))
    assert inbox.engine.gate.position["size"] == -14


async def test_history_context_message_cannot_open(inbox):
    receive(inbox, "INJ 当前开空", live=False)
    job = await inbox.claim()
    await apply(inbox, job["lease"], answer(job, symbol="INJ", side="short"))
    assert not inbox.engine.gate.writes


async def test_restart_preserves_messages_for_analysis_without_replaying_entries(inbox):
    receive(inbox, "INJ 开空")
    old = await inbox.claim()
    inbox.store.recover_analysis()
    assert not inbox.fresh(old["lease"])
    job = await inbox.claim()
    assert job["events"][0]["live"] is True
    assert job["events"][0]["entry_eligible"] is False
    await apply(inbox, job["lease"], answer(job, symbol="INJ", side="short"))
    assert not inbox.engine.gate.writes


async def test_trade_state_change_invalidates_model_result(inbox):
    trade = await open_trade(inbox)
    receive(inbox, "平掉", message=2)
    job = await inbox.claim()
    trade["version"] += 1
    inbox.store.save_trade(trade)
    with pytest.raises(Review, match="stale"):
        await apply(inbox, job["lease"], answer(job, "close", target_trade_id=trade["id"]))
    assert inbox.engine.gate.position["size"] == -14


async def test_new_message_during_entry_preflight_prevents_order(inbox):
    receive(inbox, "INJ 开空")
    job = await inbox.claim()
    original = inbox.engine.gate.get

    async def get(path, **params):
        if path == "/accounts":
            receive(inbox, "取消", message=2)
        return await original(path, **params)

    inbox.engine.gate.get = get
    await apply(inbox, job["lease"], answer(job, symbol="INJ", side="short"))
    assert not inbox.engine.gate.orders


async def test_no_matching_position_can_be_ignored_without_alert(inbox):
    receive(inbox, "JTO 平掉")
    job = await inbox.claim()
    await apply(inbox, job["lease"], answer(job, "ignore", symbol="JTO"))
    assert not inbox.engine.gate.writes
    assert inbox.store.db.execute("SELECT COUNT(*) FROM notices").fetchone()[0] == 0


def test_decision_cannot_override_risk_parameters():
    with pytest.raises(ValueError):
        Decisions.model_validate(
            {
                "decisions": [
                    {
                        "event_id": "e",
                        "action": "open",
                        "symbol": "INJ",
                        "side": "short",
                        "reason": "fixture",
                        "evidence_message_ids": [1],
                        "leverage": 100,
                    }
                ]
            }
        )
