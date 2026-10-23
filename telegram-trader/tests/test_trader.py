import copy
import json
import time

import pytest
from aiohttp.test_utils import TestClient, TestServer
from legacy_signal_fixture import parse

from trader.app import make_app
from trader.engine import Engine, Review, size_order
from trader.gate import GateError
from trader.models import Plan
from trader.store import Store

CONTRACT = {
    "quanto_multiplier": "1",
    "leverage_max": "100",
    "order_size_min": 1,
    "order_size_max": 100000,
    "taker_fee_rate": "0.0005",
    "order_price_round": "0.01",
    "enable_decimal": False,
    "in_delisting": False,
}


class Exchange:
    """Exchange simulator with independent order/position state and injected failures."""

    def __init__(self):
        self.account = {
            "currency": "USDT",
            "in_dual_mode": False,
            "enable_evolved_classic": False,
            "total": "100",
            "unrealised_pnl": "0",
            "available": "100",
            "margin_mode": 0,
            "position_mode": "single",
        }
        self.contract = copy.deepcopy(CONTRACT)
        self.position = {
            "size": 0,
            "leverage": "0",
            "lever": "7",
            "pos_margin_mode": "isolated",
            "mode": "single",
            "mark_price": "10",
            "entry_price": "10",
        }
        self.orders, self.stops, self.writes = {}, {}, []
        self.lost_response = False
        self.partial = None

    async def request(self, method, path, body=None, params=None):
        if method == "GET":
            return await self.get(path, **(params or {}))
        self.writes.append((method, path, copy.deepcopy(body), params))
        if path.endswith("/set_leverage"):
            self.position["lever"] = params["leverage"]
            self.position["pos_margin_mode"] = params["margin_mode"]
            return dict(self.position)
        if method == "POST" and path == "/orders":
            key = str(len(self.orders) + 1)
            quantity = abs(body["size"])
            if body["price"] == "0":
                filled = min(quantity, self.partial) if self.partial is not None else quantity
                direction = 1 if body["size"] > 0 else -1
                if body["reduce_only"]:
                    filled = min(filled, abs(self.position["size"]))
                    if self.position["size"] * direction >= 0:
                        filled = 0
                self.position["size"] += filled * direction
                status = "finished"
            else:
                filled, status = 0, "open"
            order = dict(body, id=key, left=quantity - filled, status=status, fill_price="10")
            self.orders[key] = order
            if self.lost_response:
                self.lost_response = False
                raise TimeoutError("injected after commit")
            return dict(order)
        if method == "POST" and path == "/price_orders":
            key = str(100 + len(self.stops))
            result = dict(body, id=key, status="open")
            self.stops[key] = result
            return dict(result)
        if method == "PUT" and path.startswith("/price_orders/amend/"):
            if "price" not in body:
                raise GateError(400, "AUTO_INVALID_PARAM_PRICE")
            result = self.stops[path.rsplit("/", 1)[1]]
            result["trigger"]["price"] = body["trigger_price"]
            return int(result["id"])
        if method == "PUT" and path.startswith("/orders/"):
            result = self.orders[path.rsplit("/", 1)[1]]
            already_filled = abs(result["size"]) - abs(result["left"])
            result.update(body)
            result["left"] = abs(result["size"]) - already_filled
            return dict(result)
        if method == "DELETE":
            collection = self.stops if path.startswith("/price_orders/") else self.orders
            result = collection[path.rsplit("/", 1)[1]]
            result["status"] = "finished"
            result["finish_as"] = "cancelled"
            return dict(result)
        raise AssertionError((method, path, body))

    async def get(self, path, **params):
        if path == "/accounts":
            return dict(self.account)
        if path.startswith("/contracts/"):
            return dict(self.contract)
        if path.startswith("/positions/"):
            return dict(self.position)
        if path in {"/orders", "/price_orders"}:
            collection = self.stops if path == "/price_orders" else self.orders
            return [dict(o) for o in collection.values() if o["status"] == "open"]
        collection = self.stops if path.startswith("/price_orders/") else self.orders
        key = path.rsplit("/", 1)[1]
        if key in collection:
            return copy.deepcopy(collection[key])
        for order in collection.values():
            if order.get("text") == key:
                return copy.deepcopy(order)
        raise GateError(404, "ORDER_NOT_FOUND")

    async def market(self, symbol):
        return [{"lowest_ask": "10", "highest_bid": "10"}]

    async def candles(self, symbol):
        return [{"t": int(time.time()), "o": "10", "h": "11", "l": "9", "c": "10", "v": "1000"}]


@pytest.fixture
def rig(tmp_path):
    store = Store(tmp_path / "state.sqlite3")
    exchange = Exchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield store, exchange, engine
    store.close()


async def signal(
    rig, text="#INJ 当前价格入场一笔空单", message=1, reply=None, live=True, edited=False
):
    store, _, engine = rig
    key = store.event(-100123, message, text, reply, time.time(), live, edited)
    if key:
        event = store.db.execute("SELECT * FROM events WHERE id=?", (key,)).fetchone()
        reference = store.reply_trade(event["channel"], reply)
        decision = parse(text, reference["symbol"].removesuffix("_USDT") if reference else None)
        if decision.action not in {"open", "ignore", "review"}:
            matches = [t for t in engine.active() if t["symbol"] == decision.symbol + "_USDT"]
            if len(matches) == 1:
                decision.target_trade_id = matches[0]["id"]
        await engine.process(event, decision)
    return store.trades()[0] if store.trades() else None


def plan():
    return Plan(sl="11", tp1="9", tp2="8", tp1_fraction="0.5", rationale="fixture levels")


@pytest.mark.parametrize(
    "text,action,symbol",
    [
        ("#INJ\n當前價格入場一筆空單", "open", "INJ"),
        ("#JTO 當前價格入場一筆多單，進行突破博弈", "open", "JTO"),
        ("#FOLKS 可以等待突破關鍵壓力位後進行右側多頭交易", "ignore", "FOLKS"),
        ("#ARIA 跌破支撐位後看空延續", "ignore", "ARIA"),
        ("INJ 補倉入場後均價8.55附近", "ignore", "INJ"),
        ("我的開倉價：8.468", "ignore", None),
        ("#FOLKS 当前价格可以选择全部止盈", "close", "FOLKS"),
        ("ARIA 利潤不錯，當前價格可以做TP1", "tp1", "ARIA"),
        ("#INJ SL放在8.75", "sl", "INJ"),
        ("#INJ 在8.63限价入场空单", "open", "INJ"),
        ("#BTC #ETH 当前价格入场一笔多单", "review", None),
        ("当前价格入场一笔多单", "review", None),
        ("當前價格可以選擇全部止盈", "review", None),
    ],
)
def test_legacy_executor_fixture_labels(text, action, symbol):
    result = parse(text)
    assert (result.action, result.symbol) == (action, symbol)


def test_sizing_equity_not_available():
    assert size_order("100", "25", "10", CONTRACT) == (14, "20.0")
    assert size_order("125", "100", "10", CONTRACT) == (17, "25.0")
    with pytest.raises(Review, match="insufficient"):
        size_order("100", "20", "10", CONTRACT)


@pytest.mark.parametrize("price", ["NaN", "Infinity", "0", "-1"])
def test_invalid_values(price):
    with pytest.raises((ValueError, Review)):
        size_order("100", "100", price, CONTRACT)


async def test_model_approved_entry_does_not_wait_for_exit_plan(rig):
    store, exchange, _ = rig
    trade = await signal(rig)
    assert trade["filled"] == 14
    assert trade["entry_price"] == "10"
    assert trade["plan_needed"] is True
    assert exchange.position["size"] == -14
    assert not exchange.stops
    assert len(exchange.orders) == 1
    assert store.db.execute("SELECT status FROM events").fetchone()[0] == "done"


async def test_equity_includes_upl(rig):
    rig[1].account["unrealised_pnl"] = "25"
    trade = await signal(rig)
    assert trade["lots"] == 17 and trade["equity"] == "125"


async def test_duplicate_and_edited_entry_never_resends(rig):
    await signal(rig)
    await signal(rig)
    await signal(rig, "#INJ 当前价格入场一笔空单！", edited=True)
    assert len(rig[1].orders) == 1


async def test_same_symbol_different_message_blocked(rig):
    await signal(rig)
    await signal(rig, message=2)
    assert len(rig[1].orders) == 1


async def test_history_never_opens(rig):
    await signal(rig, live=False)
    assert not rig[1].writes


async def test_disabled_never_trades(rig):
    rig[2].enabled = False
    await signal(rig)
    assert not rig[1].writes


@pytest.mark.parametrize(
    "field,value",
    [
        ("in_dual_mode", True),
        ("position_mode", "dual"),
        ("margin_mode", 1),
        ("currency", "BTC"),
    ],
)
async def test_unsupported_account_rejected(rig, field, value):
    rig[1].account[field] = value
    await signal(rig)
    assert not rig[1].writes


async def test_unmanaged_position_never_adopted(rig):
    rig[1].position["size"] = 1
    await signal(rig)
    assert not rig[1].writes


async def test_partial_fill_is_not_ack_size(rig):
    rig[1].partial = 7
    trade = await signal(rig)
    assert trade["lots"] == 14 and trade["filled"] == 7
    job = await rig[2].claim_plan()
    await rig[2].apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    trade = rig[0].trade(trade["id"])
    assert trade["exits"]["tp1"]["size"] == 3
    assert trade["exits"]["tp2"]["size"] == 4


async def test_lost_entry_response_reconciles_without_resend(rig):
    store, exchange, engine = rig
    exchange.lost_response = True
    trade = await signal(rig)
    assert engine.paused() and len(store.unknowns()) == 1
    assert exchange.position["size"] == -14
    await engine.poll()
    assert store.trade(trade["id"])["filled"] == 14
    assert len(exchange.orders) == 1
    assert not store.unknowns()
    assert engine.paused()  # recovery is not permission to create more risk


async def test_lost_response_404_does_not_resend(rig):
    rig[1].lost_response = True
    await signal(rig)
    rig[1].orders.clear()  # simulate unavailable correlation lookup
    writes = len(rig[1].writes)
    await rig[2].poll()
    assert len(rig[1].writes) == writes
    assert rig[0].unknowns()


async def test_restart_reuses_durable_intents(rig):
    store, exchange, _engine = rig
    exchange.lost_response = True
    trade = await signal(rig)
    restarted = Engine(store, exchange, True)
    await restarted.poll()
    assert store.trade(trade["id"])["filled"] == 14
    assert len(exchange.orders) == 1


async def test_plan_native_reduce_only_and_replay_rejected(rig):
    await signal(rig)
    job = await rig[2].claim_plan()
    await rig[2].apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    stops = list(rig[1].stops.values())
    assert stops[0]["initial"]["close"] and stops[0]["initial"]["reduce_only"]
    assert stops[0]["trigger"]["rule"] == 1  # short stop crosses upwards
    assert all(o["reduce_only"] for o in list(rig[1].orders.values())[1:])
    with pytest.raises(Review, match="stale"):
        await rig[2].apply_plan(job["trade_id"], job["version"], job["lease"], plan())


async def test_channel_stop_invalidates_ai_job(rig):
    await signal(rig)
    job = await rig[2].claim_plan()
    await signal(rig, "SL 放在10.50", message=2, reply=1)
    with pytest.raises(Review, match="stale"):
        await rig[2].apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    assert next(iter(rig[1].stops.values()))["trigger"]["price"] == "10.50"


async def test_plan_proposal_is_validated_without_orders_and_rechecked_on_apply(rig):
    await signal(rig)
    job = await rig[2].claim_plan()
    args = [job[k] for k in ("trade_id", "version", "lease")]
    before = len(rig[1].writes)
    assert rig[2].plan_proposal(*args) is None
    await rig[2].propose_plan(*args, plan())
    assert rig[2].plan_proposal(*args)["sl"] == "11"
    assert len(rig[1].writes) == before
    await signal(rig, "SL 放在10.50", message=2, reply=1)
    before = len(rig[1].writes)
    with pytest.raises(Review, match="stale"):
        rig[2].plan_proposal(*args)
    with pytest.raises(Review, match="stale"):
        await rig[2].apply_plan(*args, plan())
    assert len(rig[1].writes) == before


async def test_expired_or_disabled_plan_cannot_stage_or_execute(rig):
    await signal(rig)
    job = await rig[2].claim_plan()
    args = [job[k] for k in ("trade_id", "version", "lease")]
    trade = rig[0].trade(job["trade_id"])
    trade["plan_lease"] = time.time() - 1
    rig[0].save_trade(trade)
    before = len(rig[1].writes)
    with pytest.raises(Review, match="stale"):
        await rig[2].propose_plan(*args, plan())
    with pytest.raises(Review, match="stale"):
        await rig[2].apply_plan(*args, plan())
    job = await rig[2].claim_plan()
    rig[2].enabled = False
    with pytest.raises(Review, match="disabled"):
        await rig[2].propose_plan(job["trade_id"], job["version"], job["lease"], plan())
    assert len(rig[1].writes) == before


async def test_pause_does_not_block_close(rig):
    await signal(rig)
    rig[2].pause("operator")
    await signal(rig, "全部平仓", message=2, reply=1)
    assert rig[1].position["size"] == 0


async def test_close_cancels_pending_limit_before_market(rig):
    trade = await signal(rig, "#INJ 在10限价入场空单")
    exchange = rig[1]
    entry = exchange.orders[trade["entry_id"]]
    entry["left"] = 10
    exchange.position["size"] = -4
    await rig[2].poll()
    await signal(rig, "全部平仓", message=2, reply=1)
    assert exchange.position["size"] == 0
    paths = [(m, p) for m, p, _, _ in exchange.writes]
    assert paths.index(("DELETE", "/orders/1")) < len(paths) - 1
    await rig[2].poll()
    await rig[2].poll()
    assert rig[0].trade(trade["id"])["status"] == "closed"


async def test_plan_bad_levels_do_not_send_orders(rig):
    await signal(rig)
    job = await rig[2].claim_plan()
    writes = len(rig[1].writes)
    with pytest.raises(ValueError):
        await rig[2].apply_plan(
            job["trade_id"],
            job["version"],
            job["lease"],
            Plan(sl="9", tp1="11", tp2="12", tp1_fraction="0.5", rationale="bad"),
        )
    assert len(rig[1].writes) == writes


async def test_api_auth_and_no_entry_endpoint(rig, monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "test-only-not-a-real-secret")
    async with TestClient(TestServer(make_app(rig[2]))) as client:
        assert (await client.get("/health")).status == 200
        assert (await client.get("/status")).status == 401
        headers = {"Authorization": "Bearer test-only-not-a-real-secret"}
        assert (await client.get("/status", headers=headers)).status == 200
        assert (await client.post("/open", headers=headers)).status == 404


def test_plan_rejects_extra_keys_and_nonfinite():
    from pydantic import ValidationError

    for patch in ({"leverage": 100}, {"sl": "NaN"}, {"tp1_fraction": "1"}):
        with pytest.raises(ValidationError):
            Plan.model_validate(json.loads(plan().model_dump_json()) | patch)


def test_nontrading_question_is_ignored():
    assert parse("大家吃饭了吗？").action == "ignore"


def test_limit_entry_with_levels_stays_an_entry():
    result = parse("#INJ 在10限价入场空单 TP1:9 TP2:8 SL:11")
    assert result.action == "open" and result.price == "10"
    assert result.levels == {"tp1": "9", "tp2": "8", "sl": "11"}


async def test_management_reply_chain_resolves_original_trade(rig):
    await signal(rig)
    await signal(rig, "我的开仓价：10", message=2, reply=1)
    await signal(rig, "全部平仓", message=3, reply=2)
    assert rig[1].position["size"] == 0


async def test_close_partial_ioc_retries_only_after_terminal(rig):
    await signal(rig)
    rig[1].partial = 7
    await signal(rig, "全部平仓", message=2, reply=1)
    assert rig[1].position["size"] == -7
    for _ in range(6):
        await rig[2].poll()
    assert rig[1].position["size"] == 0
    assert rig[0].trades()[0]["status"] == "closed"


async def test_tp_partial_ioc_completes_without_over_reduction(rig):
    await signal(rig)
    job = await rig[2].claim_plan()
    await rig[2].apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    rig[1].partial = 5
    await signal(rig, "INJ 当前价格做TP1", message=2)
    for _ in range(4):
        await rig[2].poll()
    assert rig[1].position["size"] == -7
    await signal(rig, "INJ 当前价格做TP1", message=3)
    assert rig[1].position["size"] == -7


async def test_channel_tp_levels_override_plan_and_amend_orders(rig):
    await signal(rig)
    await signal(rig, "INJ TP1: 9.50 TP2: 8.50 SL: 10.50", message=2)
    job = await rig[2].claim_plan()
    await rig[2].apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    assert rig[1].orders["2"]["price"] == "9.50"
    assert rig[1].orders["3"]["price"] == "8.50"
    await signal(rig, "INJ TP1: 9.40", message=3)
    assert rig[1].orders["2"]["price"] == "9.40"


async def test_later_limit_fills_resize_exits(rig):
    trade = await signal(rig, "#INJ 在10限价入场空单")
    entry = rig[1].orders[trade["entry_id"]]
    entry["left"] = 4
    rig[1].position["size"] = -10
    await rig[2].poll()
    job = await rig[2].claim_plan()
    await rig[2].apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    entry["left"] = 0
    entry["status"] = "finished"
    rig[1].position["size"] = -14
    await rig[2].poll()
    assert rig[1].orders["2"]["size"] == 7
    assert rig[1].orders["3"]["size"] == 7


async def test_stop_renewal_creates_before_cancelling(rig):
    await signal(rig)
    await signal(rig, "INJ SL: 10.50", message=2)
    trade = rig[0].trades()[0]
    trade["exits"]["sl"]["created"] = time.time() - 83000
    rig[0].save_trade(trade)
    await rig[2].poll()
    assert rig[1].stops["100"]["status"] == "finished"
    assert rig[1].stops["101"]["status"] == "open"
    writes = [(m, p) for m, p, _, _ in rig[1].writes]
    assert writes[-2:] == [("POST", "/price_orders"), ("DELETE", "/price_orders/100")]


async def test_stop_trigger_closes_residual_position(rig):
    await signal(rig)
    await signal(rig, "INJ SL: 10.50", message=2)
    rig[1].stops["100"].update(status="finished", finish_as="succeeded")
    rig[1].position["size"] = -3  # native close only partially filled
    await rig[2].poll()
    assert rig[1].position["size"] == 0


async def test_no_model_call_can_change_leverage(rig):
    await signal(rig)
    job = await rig[2].claim_plan()
    await rig[2].apply_plan(job["trade_id"], job["version"], job["lease"], plan())
    assert [p for m, p, _, _ in rig[1].writes if p.endswith("/set_leverage")] == [
        "/positions/INJ_USDT/set_leverage"
    ]


@pytest.mark.parametrize("text", ["#INJ 不要入场空单 当前价格", "#INJ 取消当前价格入场一笔空单"])
def test_negated_entries_do_not_open(text):
    assert parse(text).action == "review"


@pytest.mark.parametrize(
    "text",
    [
        "早上好",
        "BTC 今天波动比较大",
        "FOLKS 突破多单也是成功拿下利润",
        "INJ 平仓完成",
        "#INJ 昨天当前价格入场一笔空单，今天复盘",
        "#ARIA TP1已到",
        "我的开仓价：8.468",
    ],
)
async def test_no_action_messages_have_zero_exchange_writes(rig, text):
    await signal(rig, text)
    assert not rig[1].writes
    assert rig[0].db.execute("SELECT status FROM events").fetchone()[0] == "ignore"


@pytest.mark.parametrize(
    "text",
    [
        "#INJ 不要全部平仓",
        "#INJ 当前价格入场空单？",
        "INJ 减仓一半",
        "INJ TP1 见图",
        "INJ 补仓",
        "当前价格可以选择全部止盈",
    ],
)
async def test_unresolved_actions_not_silently_ignored(rig, text):
    await signal(rig, text)
    assert not rig[1].writes
    assert rig[0].db.execute("SELECT status FROM events").fetchone()[0] == "review"
    assert rig[0].db.execute("SELECT COUNT(*) FROM notices").fetchone()[0] == 1


async def test_combined_addon_and_stop_only_changes_stop(rig):
    await signal(rig)
    await signal(rig, "INJ 在10.20挂一个补仓 SL放在10.75 具体止盈位置等我通知", message=2)
    assert len(rig[1].orders) == 1
    assert rig[1].stops["100"]["trigger"]["price"] == "10.75"


@pytest.mark.parametrize("delay", [65, 120, 600])
async def test_live_open_is_not_discarded_for_delivery_or_model_delay(rig, delay):
    store, exchange, engine = rig
    store.set("entries_started_after", time.time() - delay - 10)
    key = store.event(-100123, 1, "#INJ 当前价格入场一笔空单", None, time.time() - delay, True)
    await engine.process(
        store.db.execute("SELECT * FROM events WHERE id=?", (key,)).fetchone(),
        parse("#INJ 当前价格入场一笔空单"),
    )
    assert len(exchange.orders) == 1
    assert store.db.execute("SELECT status FROM events WHERE id=?", (key,)).fetchone()[0] == "done"


async def test_sl_receipt_crash_recovers_existing_stop(rig):
    await signal(rig)
    await signal(rig, "INJ SL放在10.50", message=2)
    trade = rig[0].trades()[0]
    trade["exits"] = {}  # simulate crash before the trade-row commit
    rig[0].save_trade(trade)
    await rig[2].poll()
    assert rig[0].trades()[0]["exits"]["sl"]["id"] == "100"
    assert len(rig[1].stops) == 1


async def test_deprecated_account_flag_does_not_reject_classic(rig):
    rig[1].account["enable_evolved_classic"] = True
    await signal(rig)
    assert len(rig[1].orders) == 1
    assert rig[1].writes[0][3] == {"leverage": "7", "margin_mode": "isolated"}


@pytest.mark.parametrize("field", ["margin_mode", "position_mode"])
async def test_missing_account_mode_fails_closed(rig, field):
    rig[1].account.pop(field, None)
    await signal(rig)
    assert not rig[1].writes


@pytest.mark.parametrize(
    "field,value", [("pos_margin_mode", "cross"), ("lever", "12"), ("mode", "dual_long")]
)
async def test_changed_position_mode_does_not_send_orders(rig, field, value):
    await signal(rig)
    rig[1].position[field] = value
    before = len(rig[1].writes)
    await signal(rig, "全部平仓", message=2, reply=1)
    assert len(rig[1].writes) == before
    assert rig[2].paused()


@pytest.mark.parametrize("side", ["long", "short"])
async def test_native_stop_complete_lifecycle(rig, side):
    direction = "多" if side == "long" else "空"
    trade = await signal(rig, f"#INJ 当前价格入场一笔{direction}单")
    job = await rig[2].claim_plan()
    exit_plan = (
        Plan(sl="9", tp1="11", tp2="12", tp1_fraction="0.5", rationale="fixture")
        if side == "long"
        else plan()
    )
    await rig[2].apply_plan(job["trade_id"], job["version"], job["lease"], exit_plan)
    rig[1].stops["100"].update(status="finished", finish_as="succeeded")
    rig[1].position["size"] = 3 if side == "long" else -3
    for _ in range(3):
        await rig[2].poll()
    assert rig[1].position["size"] == 0
    assert rig[0].trade(trade["id"])["status"] == "closed"
    assert not await rig[1].get("/orders", status="open")
    assert not await rig[1].get("/price_orders", status="open")
    assert not rig[0].unknowns()


async def test_stop_amend_preserves_market_full_close(rig):
    await signal(rig)
    await signal(rig, "INJ SL: 11", message=2)
    await signal(rig, "INJ SL: 10.5", message=3)
    operation = rig[0].db.execute("SELECT status,detail FROM events WHERE message=3").fetchone()
    assert operation["status"] == "done", operation["detail"]
    body = rig[1].writes[-1][2]
    assert body == {
        "order_id": 100,
        "size": 0,
        "price": "0",
        "close": True,
        "trigger_price": "10.5",
        "price_type": 1,
    }
