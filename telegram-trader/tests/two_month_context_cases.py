"""Real current-revision messages with deliberately constructed owned state.

These four cases supplement, never replace, the frozen 17 owned-state and 10
entry-intent goldens. Prices, orders and fills are synthetic, not a backtest.
Original source text/images/history must not be rewritten to fit the seeds.
"""

from comprehensive_cases import case, decision, trade

CASES = [
    case(
        "real_context_advertised_ETH_entry_exit_preserves_owned_short",
        [14996],
        [decision(["ignore", "review"])],
        [trade("ETH", "short", 9_000_001)],
        fixture_note=(
            "An existing ETH short has native SL/TP1/TP2. Source9000001 is only a synthetic "
            "owned-entry identity, absent from real channel history; no real source is relabelled. "
            "Its price10 is synthetic. 14996 replies to VIP promotion14979 and contains an old "
            "ETH2423 short/add-on/targets plan beside its profit screenshot and optional exit. "
            "Neither embedded entry nor management wording authorizes acting on this account."
        ),
        require_source_image=True,
    ),
    case(
        "real_context_ZETA_cancel_absent_addon_preserves_original",
        [15184],
        [decision(["ignore", "review"])],
        [dict(trade("ZETA", "long", 15179), entry="0.06378", tick="0.00001")],
        fixture_note=(
            "The original ZETA long is filled with native SL/TP1/TP2. No add-on was ever "
            "created from 15181. Its reply 15184 cancels that absent add-on and reports "
            "TP1 already reached; it does not cancel the original or command a new TP1 sale."
        ),
    ),
    case(
        "real_context_BR_exit_unowned_short_preserves_old_long",
        [15196],
        [decision(["ignore", "review"])],
        [dict(trade("BR", "long", 15163), entry="1.10", tick="0.0001")],
        fixture_note=(
            "Only the older BR long from 15163 exists, with native SL/TP1/TP2. "
            "15196 replies to the distinct bearish idea 15194, which never opened "
            "an owned short. Matching BR alone must not close the old long."
        ),
    ),
    case(
        "real_context_advertised_BTC_cancel_preserves_pending_order",
        [15355],
        [decision(["ignore", "review"])],
        [trade("BTC", "long", 15161, limit="10", market="11", fill=4)],
        fixture_note=(
            "A synthetic BTC limit entry has four filled contracts, an unfilled remainder, "
            "and native SL/TP1/TP2. Source 15161 identifies it, but this seed does not claim "
            "that its three-target instruction was supported or historically executed. "
            "15355 is a VIP-advertising album screenshot containing an old BTC cancel quote "
            "and AVA profit card, not a current instruction to cancel this account's BTC order. "
            "Price10 is a fixture value, not historical BTC pricing."
        ),
        require_source_image=True,
    ),
]

for item in CASES:
    item["expectation_scope"] = "current_revision_context_with_constructed_owned_state"
    item["state_origin"] = "synthetic_positions_orders_and_fills_not_historical_execution"

assert len(CASES) == len({item["name"] for item in CASES}) == 4


def assert_context_report(spec, report):
    """Additional acceptance checks beyond run_case's exact order/stop equality."""
    assert report["semantic_passed"] and report["execution_passed"], report
    assert report["exchange_writes"] == [], "Context-only message mutated exchange state"
    before = report["owned_before"]["A"]
    after, = report["owned_after"]
    for key in ("id", "symbol", "side", "message", "entry_id", "filled", "exits"):
        assert after[key] == before[key], (key, before[key], after[key])
    if spec.get("require_source_image"):
        assert {"op": "download", "message_id": spec["events"][0]} in report["tools"], (
            "An image-only semantic pass requires the actual frozen image to be read"
        )
