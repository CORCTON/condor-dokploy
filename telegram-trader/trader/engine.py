import asyncio
import hashlib
import json
import time
import uuid
from decimal import ROUND_DOWN, Decimal

from .gate import GateError
from .models import Plan
from .models import decimal as D

LEVERAGE = 10


class Review(Exception):
    """Do not infer or retry an unsafe/ambiguous operation."""


class Reanalyze(Review):
    """Context changed before an order write; obtain a new model decision."""


def tag(key):
    return "t-cdt-" + hashlib.sha256(key.encode()).hexdigest()[:22]


def isolated_position(position):
    return (
        position.get("pos_margin_mode") == "isolated"
        and position.get("mode") == "single"
        and D(position.get("lever", 0)) == LEVERAGE
    )


def validate_account(account):
    if (
        account.get("currency") != "USDT"
        or account.get("in_dual_mode") is not False
        or account.get("margin_mode") != 0
        or account.get("position_mode") != "single"
    ):
        raise Review(
            "requires classic USDT futures, one-way mode; account mode will not be changed"
        )


def size_order(equity, available, price, contract):
    equity, available, price = D(equity), D(available), D(price)
    multiplier = D(contract["quanto_multiplier"])
    if min(equity, price, multiplier) <= 0:
        raise Review("invalid equity/price/multiplier")
    if contract.get("enable_decimal") or contract.get("in_delisting"):
        raise Review("decimal-size or delisting contracts are not supported")
    if D(contract["leverage_max"]) < LEVERAGE:
        raise Review(f"contract does not support {LEVERAGE}x")
    margin = equity * Decimal("0.2")
    lots = int((margin * LEVERAGE / (price * multiplier)).to_integral_value(rounding=ROUND_DOWN))
    if not int(contract["order_size_min"]) <= lots <= int(contract["order_size_max"]):
        raise Review("size outside exchange limits; will not silently resize")
    fee = D(contract["taker_fee_rate"])
    if fee < 0 or available < margin + lots * price * multiplier * fee:
        raise Review("insufficient available funds for 20% equity margin plus entry fee")
    return lots, str(margin)


class Engine:
    def __init__(self, store, gate, enabled=False):
        self.store, self.gate, self.enabled = store, gate, enabled
        self.lock = asyncio.Lock()
        self.listener_ready = False
        self.pause_revision = 0

    def paused(self):
        return self.store.get("paused", True) or bool(self.store.unknowns())

    def pause(self, reason):
        self.pause_revision += 1
        self.store.set("paused", True)
        self.store.set("pause_reason", reason)

    async def resume(self):
        """Operator-only control: read-only checks, no order or leverage writes."""
        revision = self.pause_revision
        async with self.lock:
            def check():
                if not self.enabled or not self.gate.ready:
                    raise Review("执行配置或凭证未就绪，未开启跟单；请检查服务配置。")
                if not self.listener_ready:
                    raise Review("频道监听未连接，未开启跟单；请等待监听恢复后重试。")
                if self.store.unknowns():
                    raise Review("存在结果未确认的订单，未开启跟单；请先核对订单结果。")
                if any(t.get("last_error") for t in self.active()):
                    raise Review("已有仓位需要人工核查，未开启跟单；请先查看执行状态。")
                if any(
                    time.time() - self.store.get(name, 0) > 180
                    for name in ("interpreter_seen", "planner_seen")
                ):
                    raise Review("信号分析或止盈止损规划未就绪，未开启跟单；请检查 Routine。")
                if revision != self.pause_revision:
                    raise Review("检查期间收到暂停指令，保持暂停；如需开启，请重新发送开始。")

            check()
            validate_account(await self.gate.get("/accounts"))
            check()
            if self.paused():
                # Only messages received AND published after this activation may
                # open positions. A second start is idempotent, not a new cutoff.
                self.store.set("entries_started_after", time.time())
                self.store.set("pause_reason", "")
                self.store.set("paused", False)

    def active(self):
        return [
            t
            for t in self.store.trades()
            if t["status"] not in {"closed", "rejected", "deferred_entry"}
        ]

    async def write(self, trade, key, method, path, body=None, params=None):
        if not self.enabled:
            raise Review("deployment disabled; exchange writes forbidden")
        if key in {trade["id"] + ":entry", trade["id"] + ":leverage"} and self.paused():
            raise Review("new entries paused before submission")
        # Gate text is a correlation key, NOT a server-side exactly-once guarantee.
        previous = self.store.operation(key)
        if previous:
            if (
                previous["method"] != method
                or previous["path"] != path
                or json.loads(previous["body"]) != {"body": body, "params": params}
            ):
                raise Review("operation key reused with different intent")
            if previous["status"] == "confirmed":
                return json.loads(previous["result"])
            raise Review("previous operation is unconfirmed/rejected; no blind retry")
        self.store.intent(key, trade["id"], method, path, {"body": body, "params": params})
        try:
            result = await self.gate.request(method, path, body=body, params=params)
        except GateError as exc:
            # Only explicit client-side rejection is known NOT to be accepted.
            if 400 <= exc.status < 500 and exc.status not in {408, 409, 429}:
                self.store.result(key, {"error": exc.label}, "rejected")
            else:
                self.pause("exchange write outcome unknown")
            raise
        except Exception:
            self.pause("exchange write outcome unknown")
            raise
        self.store.result(key, result)
        return result

    async def process(self, event, signal):
        """Execute a validated model decision; never infer intent from text here."""
        async with self.lock:
            await self.execute_decision(event, signal, self.store.watermark(event["channel"]))

    async def execute_decision(self, event, signal, watermark):
        current = self.store.db.execute(
            "SELECT status FROM events WHERE id=?", (event["id"],)
        ).fetchone()
        if current is None or current["status"] == "superseded":
            return
        payload = json.loads(event["payload"])
        self.store.finish(event["id"], "processing")
        try:
            if signal.action in {"ignore", "review"}:
                if signal.action == "review":
                    self.store.notice(f"Signal needs review: {event['message']} / {signal.reason}")
                self.store.finish(event["id"], signal.action, signal.reason)
                return
            if not self.enabled:
                raise Review("deployment disabled")
            if not payload["live"]:
                raise Review("historical/offline message: never auto-replay")
            if self.store.watermark(event["channel"]) != watermark:
                raise Reanalyze("new channel message arrived; reanalyze before execution")
            if signal.action == "open":
                if not payload.get("entry_eligible", payload["live"]):
                    raise Review("recovered message: no catch-up entry")
                started_after = self.store.get("entries_started_after", 0)
                if min(event["received"], payload["source_time"]) <= started_after:
                    raise Review("entry predates operator activation; no catch-up entry")
                if self.store.queued():
                    raise Reanalyze("unread channel backlog; reanalyze before entry")
                if time.time() - payload["source_time"] > 60:
                    raise Review("entry signal older than 60 seconds; no new entry")
                if payload["edited"]:
                    raise Review("edited entry: never open again automatically")
                if self.paused() or not self.listener_ready:
                    raise Review("new entries paused or listener not ready")
                await self.open(event, signal, watermark)
            else:
                trade = self.store.trade(signal.target_trade_id)
                if (
                    not trade
                    or trade["status"] in {"closed", "rejected"}
                    or trade["channel"] != event["channel"]
                    or (signal.symbol and trade["symbol"] != signal.symbol + "_USDT")
                ):
                    raise Review("no uniquely associated active trade")
                trade["version"] += 1
                self.store.save_trade(trade)
                if signal.action == "sl":
                    await self.stop(trade, D(signal.price), "channel")
                elif signal.action == "targets":
                    await self.targets(trade, signal.levels)
                elif signal.action == "close":
                    await self.close_trade(trade)
                elif signal.action == "cancel":
                    await self.cancel_entry(trade)
                else:
                    await self.take_now(trade, signal.action)
            self.store.finish(event["id"], "done")
            if signal.reason:
                self.store.notice(f"Signal {event['message']}: {signal.reason}")
        except Reanalyze:
            # Only pre-write context invalidation reaches here. Never reset an
            # event that has been superseded by a newer edit during the await.
            with self.store.db:
                self.store.db.execute(
                    "UPDATE events SET status='queued',detail='' WHERE id=? AND status='processing'",
                    (event["id"],),
                )
            raise
        except (Review, GateError, OSError, TimeoutError, ValueError, KeyError) as exc:
            detail = (
                str(exc) if isinstance(exc, (Review, GateError, ValueError)) else type(exc).__name__
            )
            self.store.finish(event["id"], "review", detail)
            self.store.notice(f"Signal {event['message']} requires review: {detail}")

    async def open(self, event, signal, watermark):
        trade_id = hashlib.sha256(f"{event['channel']}:{event['message']}".encode()).hexdigest()[
            :24
        ]
        previous = self.store.trade(trade_id)
        if previous and (
            previous["status"] != "deferred_entry"
            or self.store.operation(trade_id + ":entry")
            or self.store.unknowns()
        ):
            raise Review("entry already recorded")
        symbol = signal.symbol + "_USDT"
        if any(t["symbol"] == symbol for t in self.active()):
            raise Review("same-symbol trade already active; add-ons disabled")
        account, position, orders, triggers, contract, tickers = await asyncio.gather(
            self.gate.get("/accounts"),
            self.gate.get(f"/positions/{symbol}"),
            self.gate.get("/orders", contract=symbol, status="open"),
            self.gate.get("/price_orders", contract=symbol, status="open"),
            self.gate.get(f"/contracts/{symbol}"),
            self.gate.market(symbol),
        )
        validate_account(account)
        if D(position["size"]) != 0:
            raise Review("unmanaged position exists")
        if orders:
            raise Review("unmanaged open orders exist")
        if triggers:
            raise Review("unmanaged trigger orders exist")
        if len(tickers) != 1:
            raise Review("symbol is not uniquely listed")
        reference = tickers[0]["lowest_ask" if signal.side == "long" else "highest_bid"]
        price = D(signal.price or reference)
        tick = D(contract["order_price_round"])
        if signal.price and (price <= 0 or price % tick != 0):
            raise Review("limit price not on exchange tick; no silent price change")
        equity = D(account["total"]) + D(account["unrealised_pnl"])
        lots, margin = size_order(equity, account["available"], price, contract)
        if self.store.watermark(event["channel"]) != watermark:
            raise Reanalyze("new message during entry checks; reanalyze before entry")
        trade = {
            "id": trade_id,
            "channel": event["channel"],
            "message": event["message"],
            "symbol": symbol,
            "side": signal.side,
            "status": "preparing",
            "version": 1,
            "equity": str(equity),
            "margin_budget": margin,
            "lots": lots,
            "filled": 0,
            "entry_price": "0",
            "entry_id": None,
            "entry_terminal": False,
            "tick": str(tick),
            "multiplier": contract["quanto_multiplier"],
            "exits": {},
            "plan": None,
            "retired_stops": [],
            "channel_levels": signal.levels,
            "plan_needed": False,
            "plan_lease": 0,
            "source_sl": False,
            "close_requested": False,
            "source_text": json.loads(event["payload"])["text"],
            "created": time.time(),
        }
        self.store.save_trade(trade)
        try:
            await self.write(
                trade,
                trade_id + ":leverage",
                "POST",
                f"/positions/{symbol}/set_leverage",
                params={"leverage": str(LEVERAGE), "margin_mode": "isolated"},
            )
            verified = await self.gate.get(f"/positions/{symbol}")
            if not isolated_position(verified) or D(verified["size"]) != 0:
                raise Review(f"isolated {LEVERAGE}x could not be verified")
            if self.store.watermark(event["channel"]) != watermark:
                raise Reanalyze("new message before order submission; reanalyze before entry")
            if time.time() - json.loads(event["payload"])["source_time"] > 60:
                raise Review("entry signal older than 60 seconds; no new entry")
            body = {
                "contract": symbol,
                "size": lots if signal.side == "long" else -lots,
                "price": signal.price or "0",
                "tif": "gtc" if signal.price else "ioc",
                "reduce_only": False,
                "text": tag(trade_id + ":entry"),
            }
            order = await self.write(trade, trade_id + ":entry", "POST", "/orders", body)
            trade["entry_id"] = str(order["id"])
            trade["status"] = "working"
            self.store.save_trade(trade)
            self.store.notice(
                f"Entry accepted: {symbol} {signal.side}, {lots} contracts, isolated {LEVERAGE}x. "
                f"Margin budget {margin} USDT (20% of equity {equity}). Fill not yet confirmed; no SL yet."
            )
            await self.refresh(trade)
            if signal.levels and trade["filled"]:
                await self.targets(trade, signal.levels)
        except Reanalyze:
            # Leverage may already be confirmed, but no entry was submitted.
            # Keep its durable operation and re-use it only after fresh analysis.
            trade["status"] = "deferred_entry"
            self.store.save_trade(trade)
            raise
        except Exception:
            entry_op = self.store.operation(trade_id + ":entry")
            if not entry_op or entry_op["status"] == "rejected":
                trade["status"] = "rejected"
                self.store.save_trade(trade)
            raise

    async def order(self, order_id):
        try:
            return await self.gate.get(f"/orders/{order_id}")
        except GateError as exc:
            if exc.status != 404:
                raise
            # Gate can temporarily hide cancelled zero-fill orders. Only a
            # durable, confirmed terminal receipt is proof; 404 alone is not.
            row = self.store.db.execute(
                "SELECT result FROM operations WHERE method='DELETE' AND path=? "
                "AND status='confirmed' ORDER BY created DESC LIMIT 1",
                (f"/orders/{order_id}",),
            ).fetchone()
            if row:
                result = json.loads(row[0])
                if str(result.get("id")) == str(order_id) and result.get("status") == "finished":
                    return result
            raise

    async def cancel_regular(self, trade, order_id, purpose):
        order = await self.order(order_id)
        if order["status"] == "open":
            try:
                await self.write(
                    trade,
                    f"{trade['id']}:cancel:{order_id}:{purpose}",
                    "DELETE",
                    f"/orders/{order_id}",
                )
            except GateError as exc:
                if exc.label not in {"ORDER_NOT_FOUND", "ORDER_CLOSED"}:
                    raise
            order = await self.order(order_id)
        if order["status"] == "open":
            raise Review("cancellation not confirmed")
        return order

    async def owned_position(self, trade):
        position = await self.gate.get(f"/positions/{trade['symbol']}")
        size = D(position["size"])
        sign = 1 if trade["side"] == "long" else -1
        if size * sign < 0 or abs(size) > trade["lots"] or not isolated_position(position):
            self.pause("position ownership/mode mismatch")
            raise Review("position ownership/mode mismatch; no orders sent")
        if size != size.to_integral_value():
            raise Review("unsupported fractional position size")
        return position

    async def refresh(self, trade):
        if trade["status"] == "preparing":
            op = self.store.operation(trade["id"] + ":entry")
            if not op or op["status"] != "confirmed":
                return
            trade["entry_id"] = str(json.loads(op["result"])["id"])
            trade["status"] = "working"
        order = await self.order(trade["entry_id"])
        filled = int(abs(D(order["size"])) - abs(D(order["left"])))
        trade["entry_terminal"] = order["status"] != "open"
        if filled > trade["filled"]:
            trade["filled"] = filled
            trade["entry_price"] = str(D(order["fill_price"]))
            trade["version"] += 1
            trade["plan_needed"] = trade["plan"] is None
            self.store.save_trade(trade)
            self.store.notice(
                f"Fill confirmed: {trade['symbol']} {filled} contracts @ {trade['entry_price']}. "
                "AI exit planning queued; channel intent was already interpreted."
            )
        position = await self.owned_position(trade)
        quantity = int(abs(D(position["size"])))
        trade["position_size"] = quantity
        if quantity and trade["filled"] == 0:
            raise Review("position exists before owned fill is confirmed")
        channel_sl = trade.get("channel_levels", {}).get("sl")
        current_sl = trade["exits"].get("sl")
        if (
            quantity
            and channel_sl
            and (
                not trade["source_sl"] or not current_sl or D(current_sl["price"]) != D(channel_sl)
            )
        ):
            await self.stop(trade, D(channel_sl), "channel")
        # A fill may become visible in orders before positions. Do not declare it
        # closed in the same poll; require two independent zero observations.
        if quantity == 0 and (trade["filled"] > 0 or trade["entry_terminal"]):
            trade["zero_reads"] = trade.get("zero_reads", 0) + 1
            if trade["zero_reads"] >= 2:
                await self.cleanup(trade)
        else:
            trade["zero_reads"] = 0
        if trade.get("close_requested") and quantity:
            await self.close_trade(trade)
        for name, exit_order in list(trade["exits"].items()):
            if name != "sl" and not exit_order.get("done"):
                result = await self.order(exit_order["id"])
                exit_order["filled"] = int(abs(D(result["size"])) - abs(D(result["left"])))
                if exit_order["filled"] and not trade["entry_terminal"]:
                    await self.cancel_regular(trade, trade["entry_id"], "exit-started")
                    trade["entry_terminal"] = True
                    self.store.save_trade(trade)
                    # The entry may fill again while its cancellation is in
                    # flight. Refresh the terminal receipt before reallocating.
                    await self.refresh(trade)
                    return
                if result["status"] != "open":
                    exit_order["done"] = True
            if name != "sl" and exit_order.get("take_pending"):
                await self.take_now(trade, name)
        self.store.save_trade(trade)
        if trade["plan"] and trade["status"] == "working" and quantity:
            await self.resize_targets(trade)

    async def resize_targets(self, trade):
        """Allocate later limit fills without leaving the additional fill orphaned."""
        if trade.get("close_requested"):
            return
        allocations = [trade["exits"][name] for name in ("tp1", "tp2") if name in trade["exits"]]
        if any(item.get("take_attempt", 0) > 0 and "take_filled" not in item for item in allocations):
            raise Review("legacy TP reduction needs fill reconciliation before reallocating")
        if any(
            item.get("done")
            and not item.get("take_pending")
            and item.get("filled", 0) + item.get("take_filled", 0) < item["size"]
            for item in allocations
        ):
            sl = trade["exits"].get("sl")
            if sl:
                result = await self.gate.get(f"/price_orders/{sl['id']}")
                if result["status"] != "open" and result.get("finish_as") == "succeeded":
                    await self.close_trade(trade)
                    return
                if (
                    result["status"] == "open" and time.time() - sl["created"] > 82800
                ) or (result["status"] != "open" and result.get("finish_as") == "expired"):
                    await self.renew_stop(trade)
            raise Review("TP closed before its allocation completed; manual review required")
        if (
            not trade["plan_needed"]
            and allocations
            and all(
                item.get("done")
                and not item.get("take_pending")
                and item.get("filled", 0) + item.get("take_filled", 0) >= item["size"]
                for item in allocations
            )
        ):
            # All allocated exits filled, but the entry cancellation raced a
            # further fill. Complete the existing full-exit intent, reduce-only.
            await self.close_trade(trade)
            return
        first = int(
            (D(trade["filled"]) * D(trade["plan"]["tp1_fraction"])).to_integral_value(
                rounding=ROUND_DOWN
            )
        )
        tp1 = trade["exits"].get("tp1")
        tp2 = trade["exits"].get("tp2")
        if tp1 and tp1.get("done") and not tp1.get("take_pending"):
            first = tp1.get("filled", 0) + tp1.get("take_filled", 0)
        elif tp2 and tp2.get("done") and not tp2.get("take_pending"):
            first = trade["filled"] - tp2.get("filled", 0) - tp2.get("take_filled", 0)
        for name, target in (("tp1", first), ("tp2", trade["filled"] - first)):
            item = trade["exits"].get(name)
            if not item:
                if target:
                    await self.place_target(trade, name, target, trade["plan"][name])
                continue
            if item.get("done") or item.get("take_pending") or target <= item["size"]:
                continue
            result = await self.order(item["id"])
            if result["status"] != "open":
                continue
            signed = -target if trade["side"] == "long" else target
            await self.write(
                trade,
                f"{trade['id']}:{name}:size:{target}",
                "PUT",
                f"/orders/{item['id']}",
                {"size": signed},
            )
            item["size"] = target
            self.store.save_trade(trade)

    async def place_target(self, trade, name, size, price):
        key = f"{trade['id']}:{name}:initial"
        result = await self.write(
            trade,
            key,
            "POST",
            "/orders",
            {
                "contract": trade["symbol"],
                "size": -size if trade["side"] == "long" else size,
                "price": str(price),
                "tif": "gtc",
                "reduce_only": True,
                "text": tag(key),
            },
        )
        trade["exits"][name] = {
            "id": str(result["id"]),
            "size": size,
            "done": False,
            "filled": 0,
        }
        self.store.save_trade(trade)

    async def cleanup(self, trade):
        # Cancel the unfilled entry FIRST: an exit must not leave a latent re-entry.
        if trade["entry_id"]:
            await self.cancel_regular(trade, trade["entry_id"], "cleanup")
        for name, item in trade["exits"].items():
            if name == "sl":
                result = await self.gate.get(f"/price_orders/{item['id']}")
                if result["status"] == "open":
                    await self.write(
                        trade,
                        f"{trade['id']}:cancel-sl:{item['id']}",
                        "DELETE",
                        f"/price_orders/{item['id']}",
                    )
            else:
                await self.cancel_regular(trade, item["id"], "cleanup")
        await self.cancel_retired_stops(trade)
        # Re-read after cancellation in case the entry filled during the race.
        position = await self.owned_position(trade)
        if D(position["size"]) != 0:
            trade["close_requested"] = True
            return
        trade["status"] = "closed"
        trade["plan_needed"] = False
        trade["version"] += 1
        self.store.notice(f"Trade closed and remaining owned orders cancelled: {trade['symbol']}")

    async def stop(self, trade, price, source, *, replace=False):
        if price <= 0 or price % D(trade["tick"]) != 0:
            raise Review("SL not a valid exchange tick")
        if source == "channel":
            trade.setdefault("channel_levels", {})["sl"] = str(price)
            self.store.save_trade(trade)
        position = await self.owned_position(trade)
        if D(position["size"]) == 0:
            if replace:
                # A close can finish after refresh but before SL renewal.
                return
            if source == "channel" and not trade["entry_terminal"]:
                self.store.notice(f"Channel SL saved pending fill: {trade['symbol']} @ {price}.")
                return
            raise Review("no filled position for stop")
        market = D(position["mark_price"])
        if (trade["side"] == "long" and price >= market) or (
            trade["side"] == "short" and price <= market
        ):
            raise Review("SL already crossed; will not silently reinterpret as market close")
        existing = trade["exits"].get("sl")
        if existing and D(existing["price"]) == price and not replace:
            current = await self.gate.get(f"/price_orders/{existing['id']}")
            if current["status"] != "open":
                raise Review("existing SL no longer pending; reconcile position first")
            if source == "channel":
                trade["source_sl"] = True
                self.store.save_trade(trade)
            return
        key = f"{trade['id']}:sl:{trade['version']}:{source}:{price}"
        if replace:
            if not existing:
                raise Review("SL replacement requires the previous stop receipt")
            prefix = f"{trade['id']}:sl-replace:{existing['id']}:"
            rejected = self.store.db.execute(
                "SELECT COUNT(*) FROM operations WHERE trade_id=? AND id LIKE ? "
                "AND status='rejected'",
                (trade["id"], prefix + "%"),
            ).fetchone()[0]
            # Only a confirmed rejection permits another attempt. Unknown or
            # confirmed receipts reuse the same intent, including after restart.
            key = prefix + str(rejected)
        if existing and not replace:
            result = await self.gate.get(f"/price_orders/{existing['id']}")
            if result["status"] != "open":
                raise Review("existing SL no longer pending; reconcile position first")
            await self.write(
                trade,
                key,
                "PUT",
                f"/price_orders/amend/{existing['id']}",
                {
                    "order_id": int(existing["id"]),
                    "size": 0,
                    "price": "0",
                    "close": True,
                    "trigger_price": str(price),
                    "price_type": 1,
                },
            )
            existing["price"] = str(price)
        else:
            result = await self.write(
                trade,
                key,
                "POST",
                "/price_orders",
                {
                    "initial": {
                        "contract": trade["symbol"],
                        "size": 0,
                        "price": "0",
                        "tif": "ioc",
                        "close": True,
                        "reduce_only": True,
                        "text": tag(key),
                    },
                    "trigger": {
                        "strategy_type": 0,
                        "price_type": 1,
                        "price": str(price),
                        "rule": 2 if trade["side"] == "long" else 1,
                        "expiration": 86400,
                    },
                    "order_type": "plan-close-long-position"
                    if trade["side"] == "long"
                    else "plan-close-short-position",
                },
            )
            trade["exits"]["sl"] = {
                "id": str(result["id"]),
                "price": str(price),
                "created": time.time(),
            }
            if existing:
                trade.setdefault("retired_stops", []).append(existing)
        trade["source_sl"] = trade["source_sl"] or source == "channel"
        self.store.save_trade(trade)
        self.store.notice(f"Exchange SL accepted: {trade['symbol']} @ {price} ({source}).")

    async def targets(self, trade, levels):
        for name, price in levels.items():
            if D(price) <= 0 or D(price) % D(trade["tick"]) != 0:
                raise Review("channel target not on exchange tick")
            if name != "sl" and trade["filled"]:
                entry = D(trade["entry_price"])
                if (trade["side"] == "long" and D(price) <= entry) or (
                    trade["side"] == "short" and D(price) >= entry
                ):
                    raise Review("TP is on the loss side of actual entry")
        trade.setdefault("channel_levels", {}).update(levels)
        self.store.save_trade(trade)
        for name, price in levels.items():
            if name == "sl":
                await self.stop(trade, D(price), "channel")
            elif name in trade["exits"]:
                item = trade["exits"][name]
                current = await self.order(item["id"])
                if current["status"] != "open":
                    raise Review("TP already completed/cancelled; will not recreate it")
                await self.write(
                    trade,
                    f"{trade['id']}:{name}:amend:{trade['version']}",
                    "PUT",
                    f"/orders/{item['id']}",
                    {"price": price},
                )
        # Before a plan exists, record TP prices but leave reduction fractions to
        # the planner; never invent a fraction just to satisfy an incomplete signal.

    def plan_trade(self, trade_id, version, lease):
        trade = self.store.trade(trade_id)
        if (
            not self.enabled
            or not trade
            or trade["status"] != "working"
            or trade["version"] != version
            or trade.get("plan_token") != lease
            or trade.get("plan_lease", 0) <= time.time()
            or not trade["plan_needed"]
        ):
            raise Review("stale, disabled or already applied AI plan")
        if trade.get("close_requested"):
            raise Review("trade is closing")
        return trade

    def validate_plan(self, trade, plan):
        values = json.loads(plan.model_dump_json()) | trade.get("channel_levels", {})
        plan = Plan.model_validate(values)
        existing = trade["exits"].get("sl")
        protected_stop = trade["source_sl"] and existing and D(existing["price"]) == plan.sl
        plan.validate_side(trade["side"], D(trade["entry_price"]), validate_stop=not protected_stop)
        for price in (plan.sl, plan.tp1, plan.tp2):
            if price % D(trade["tick"]) != 0:
                raise Review("AI price not on exchange tick")
        return plan

    async def propose_plan(self, trade_id, version, lease, plan):
        async with self.lock:
            trade = self.plan_trade(trade_id, version, lease)
            plan = self.validate_plan(trade, plan)
            proposal = json.loads(plan.model_dump_json())
            if trade.get("plan_proposal") is not None and trade["plan_proposal"] != proposal:
                raise Review("accepted exit plan cannot be changed")
            trade["plan_proposal"] = proposal
            self.store.save_trade(trade)

    def plan_proposal(self, trade_id, version, lease):
        return self.plan_trade(trade_id, version, lease).get("plan_proposal")

    async def apply_plan(self, trade_id, version, lease, plan):
        async with self.lock:
            trade = self.plan_trade(trade_id, version, lease)
            plan = self.validate_plan(trade, plan)
            if trade.get("plan_proposal") != json.loads(plan.model_dump_json()):
                raise Review("execution requires the accepted exit-plan proposal")
            position = await self.owned_position(trade)
            quantity = int(abs(D(position["size"])))
            if quantity <= 0:
                raise Review("position is no longer open")
            if trade["source_sl"]:
                existing = trade["exits"].get("sl")
                if not existing:
                    raise Review("channel SL receipt missing")
                current = await self.gate.get(f"/price_orders/{existing['id']}")
                if current["status"] != "open" or D(current["trigger"]["price"]) != plan.sl:
                    raise Review("channel SL is no longer active at the planned price")
            trade["plan"] = json.loads(plan.model_dump_json())
            self.store.save_trade(trade)
            if not trade["source_sl"]:
                await self.stop(trade, plan.sl, "ai")
            # Native reduce-only limits; no polling/LLM in the exchange execution path.
            first = int((D(quantity) * plan.tp1_fraction).to_integral_value(rounding=ROUND_DOWN))
            sizes = {"tp1": first, "tp2": quantity - first}
            for name, price in (("tp1", plan.tp1), ("tp2", plan.tp2)):
                if not sizes[name] or name in trade["exits"]:
                    continue
                await self.place_target(trade, name, sizes[name], price)
            trade["plan_needed"] = False
            self.store.save_trade(trade)

    async def take_now(self, trade, name):
        item = trade["exits"].get(name)
        if not item:
            raise Review("TP allocation not established yet; no guessed reduction fraction")
        if item.get("taken_now"):
            return
        if item.get("take_attempt", 0) > 0 and "take_filled" not in item:
            raise Review("legacy TP reduction needs fill reconciliation before retrying")
        if not trade["entry_terminal"]:
            await self.cancel_regular(trade, trade["entry_id"], "take-now")
            trade["entry_terminal"] = True
            self.store.save_trade(trade)
            # Account for fills racing the cancellation before sizing the TP.
            await self.refresh(trade)
            item = trade["exits"][name]
        result = await self.cancel_regular(trade, item["id"], "take-now")
        remaining = max(0, int(abs(D(result["left"]))) - item.get("take_filled", 0))
        attempt = item.get("take_attempt", 0)
        key = f"{trade['id']}:{name}:take-now:{attempt}"
        previous = self.store.operation(key)
        if previous:
            if previous["status"] != "confirmed":
                raise Review("TP reduction outcome unknown")
            reduced = await self.order(str(json.loads(previous["result"])["id"]))
            if reduced["status"] == "open":
                return
            remaining = int(abs(D(reduced["left"])))
            item["take_filled"] = (
                item.get("take_filled", 0) + int(abs(D(reduced["size"]))) - remaining
            )
            attempt += 1
            item["take_attempt"] = attempt
        position = await self.owned_position(trade)
        remaining = min(remaining, int(abs(D(position["size"]))))
        if remaining:
            key = f"{trade['id']}:{name}:take-now:{attempt}"
            item["take_pending"] = True
            self.store.save_trade(trade)
            await self.write(
                trade,
                key,
                "POST",
                "/orders",
                {
                    "contract": trade["symbol"],
                    "size": -remaining if trade["side"] == "long" else remaining,
                    "price": "0",
                    "tif": "ioc",
                    "reduce_only": True,
                    "text": tag(key),
                },
            )
        else:
            item["taken_now"] = True
            item["take_pending"] = False
        self.store.save_trade(trade)

    async def cancel_retired_stops(self, trade):
        for item in list(trade.get("retired_stops", [])):
            result = await self.gate.get(f"/price_orders/{item['id']}")
            if result["status"] == "open":
                await self.write(
                    trade,
                    f"{trade['id']}:retire-sl:{item['id']}",
                    "DELETE",
                    f"/price_orders/{item['id']}",
                )
            trade["retired_stops"].remove(item)
            self.store.save_trade(trade)

    async def renew_stop(self, trade):
        old = trade["exits"]["sl"]
        # Old native stop remains active until the replacement is accepted.
        await self.stop(
            trade, D(old["price"]), "channel" if trade["source_sl"] else "ai", replace=True
        )
        await self.cancel_retired_stops(trade)

    async def cancel_entry(self, trade):
        """Cancel unfilled entry quantity, preserving fills and their protective exits."""
        await self.cancel_regular(trade, trade["entry_id"], "channel-cancel")
        await self.refresh(trade)
        if trade.get("position_size"):
            self.store.notice(
                f"Entry cancellation checked: {trade['symbol']}; a filled position remains. "
                "Cancellation is not a close instruction; protection is retained."
            )

    async def close_trade(self, trade):
        trade["close_requested"] = True
        trade["plan_needed"] = False
        self.store.save_trade(trade)
        await self.cancel_regular(trade, trade["entry_id"], "close")
        position = await self.owned_position(trade)
        quantity = int(abs(D(position["size"])))
        if quantity:
            # After a terminal partial IOC, another reduction may be needed. Each
            # retry requires observing the previous order terminal, never a timeout.
            attempt = trade.get("close_attempt", 0)
            key = f"{trade['id']}:close:{attempt}"
            previous = self.store.operation(key)
            if previous and previous["status"] == "confirmed":
                order = await self.order(str(json.loads(previous["result"])["id"]))
                if order["status"] == "open":
                    return
                attempt += 1
                trade["close_attempt"] = attempt
                self.store.save_trade(trade)
                key = f"{trade['id']}:close:{attempt}"
            await self.write(
                trade,
                key,
                "POST",
                "/orders",
                {
                    "contract": trade["symbol"],
                    "size": -quantity if trade["side"] == "long" else quantity,
                    "price": "0",
                    "tif": "ioc",
                    "reduce_only": True,
                    "text": tag(key),
                },
            )
        self.store.save_trade(trade)

    async def reconcile_unknowns(self):
        for op in self.store.unknowns():
            # GET by custom text can be unavailable. A 404 never authorizes resend.
            if op["method"] == "POST" and op["path"] == "/orders":
                body = json.loads(op["body"])["body"]
                try:
                    result = await self.order(body["text"])
                except GateError:
                    continue
                self.store.result(op["id"], result)
                self.store.notice(
                    "Previously uncertain order found. New entries remain paused pending review."
                )
            elif op["method"] == "POST" and op["path"] == "/price_orders":
                body = json.loads(op["body"])["body"]
                try:
                    result = await self.find_stop_receipt(body)
                except (GateError, OSError, TimeoutError):
                    continue
                if result is not None:
                    self.store.result(op["id"], result)
                    self.store.notice(
                        "Previously uncertain stop found by exact intent. "
                        "New entries remain paused pending review."
                    )

    async def find_stop_receipt(self, body):
        """Read both complete status lists; text alone is not proof of the intent."""
        matches = {}
        for status in ("open", "finished"):
            offset = 0
            while True:
                rows = await self.gate.get(
                    "/price_orders",
                    contract=body["initial"]["contract"],
                    status=status,
                    limit=100,
                    offset=offset,
                )
                for row in rows:
                    if row.get("initial", {}).get("text") == body["initial"]["text"]:
                        matches[str(row["id"])] = row
                if len(rows) < 100:
                    break
                offset += len(rows)
        if len(matches) != 1:
            return None
        result = next(iter(matches.values()))
        if result.get("order_type") != body["order_type"]:
            return None
        numeric = {"size", "price", "strategy_type", "price_type", "rule", "expiration"}
        for section in ("initial", "trigger"):
            for key, expected in body[section].items():
                values = result.get(section, {})
                response_key = {"close": "is_close", "reduce_only": "is_reduce_only"}.get(key, key)
                actual = values.get(response_key, values.get(key))
                if actual is None:
                    return None
                if key in numeric:
                    try:
                        same = D(actual) == D(expected)
                    except ValueError:
                        return None
                    if not same:
                        return None
                elif actual != expected:
                    return None
        return result

    def recover_exits(self, trade):
        """Recover a crash after exchange confirmation but before the trade row commit."""
        rows = self.store.db.execute(
            "SELECT * FROM operations WHERE trade_id=? AND status='confirmed' ORDER BY created",
            (trade["id"],),
        ).fetchall()
        deleted = {
            r["path"].rsplit("/", 1)[1]
            for r in rows
            if r["method"] == "DELETE" and r["path"].startswith("/price_orders/")
        }
        retired = {item["id"] for item in trade.get("retired_stops", [])}
        for row in rows:
            body = json.loads(row["body"])["body"]
            result = json.loads(row["result"])
            if row["method"] != "POST":
                continue
            existing = trade["exits"].get("sl")
            if (
                existing
                and row["path"] == "/price_orders"
                and row["id"].startswith(f"{trade['id']}:sl-replace:{existing['id']}:")
                and str(result["id"]) not in deleted | retired
            ):
                trade.setdefault("retired_stops", []).append(existing)
                retired.add(existing["id"])
                del trade["exits"]["sl"]
            if (
                row["path"] == "/price_orders"
                and str(result["id"]) not in deleted | retired
                and not trade["exits"].get("sl")
            ):
                trade["exits"]["sl"] = {
                    "id": str(result["id"]),
                    "price": body["trigger"]["price"],
                    "created": row["created"],
                }
            for name in ("tp1", "tp2"):
                if row["id"] == f"{trade['id']}:{name}:initial" and name not in trade["exits"]:
                    trade["exits"][name] = {
                        "id": str(result["id"]),
                        "size": abs(body["size"]),
                        "done": False,
                        "filled": 0,
                    }
        self.store.save_trade(trade)

    async def poll(self):
        async with self.lock:
            await self.reconcile_unknowns()
        for snapshot in self.active():
            if self.store.queued():
                break  # prioritize channel events between position reconciliations
            async with self.lock:
                trade = self.store.trade(snapshot["id"])
                try:
                    self.recover_exits(trade)
                    await self.refresh(trade)
                    sl = trade["exits"].get("sl")
                    if sl and trade["status"] == "working":
                        result = await self.gate.get(f"/price_orders/{sl['id']}")
                        if result["status"] == "open" and time.time() - sl["created"] > 82800:
                            await self.renew_stop(trade)
                        if result["status"] != "open" and trade.get("position_size", 0):
                            # Expiration/rejection is NOT interpreted as a request to close.
                            # Re-arm only after a confirmed non-execution terminal status.
                            if result.get("finish_as") == "expired":
                                await self.renew_stop(trade)
                            elif result.get("finish_as") == "succeeded":
                                await self.close_trade(trade)
                            else:
                                raise Review("native stop no longer active; manual review required")
                    if trade.get("retired_stops") and trade["exits"].get("sl"):
                        await self.cancel_retired_stops(trade)
                    if trade.pop("last_error", None):
                        self.store.save_trade(trade)
                except Exception as exc:  # noqa: BLE001 -- isolate each position and pause on any failure
                    label = str(exc) if isinstance(exc, (Review, GateError)) else type(exc).__name__
                    if trade.get("last_error") != label:
                        trade["last_error"] = label
                        self.store.save_trade(trade)
                        self.store.notice(f"Reconciliation {trade['symbol']}: {label}")
                    self.pause("reconciliation requires review")

    async def claim_plan(self):
        async with self.lock:
            for trade in self.active():
                if (
                    trade["status"] == "working"
                    and trade["plan_needed"]
                    and trade["filled"]
                    and trade["plan_lease"] < time.time()
                    and not trade.get("close_requested")
                ):
                    token = uuid.uuid4().hex
                    trade.update(plan_token=token, plan_lease=time.time() + 120, plan_proposal=None)
                    self.store.save_trade(trade)
                    job = {
                        "trade_id": trade["id"],
                        "version": trade["version"],
                        "lease": token,
                        "symbol": trade["symbol"],
                        "side": trade["side"],
                        "entry": trade["entry_price"],
                        "tick": trade["tick"],
                        "filled_contracts": trade["filled"],
                        "channel_text_untrusted": trade["source_text"],
                        "channel_levels": trade.get("channel_levels", {}),
                        "confirmed_channel_stop": trade["exits"].get("sl")
                        if trade["source_sl"] else None,
                        "output_schema": Plan.model_json_schema(),
                    }
                    break
            else:
                return None
        job["candles_5m"] = await self.gate.candles(job["symbol"])
        return job
