"""Independent real-message oracles with deliberately constructed owned state.

Messages are current visible revisions, not original point-in-time Telegram data.
Every owned position, price, fill and allocation below is a synthetic test seed,
NOT evidence that a historical trade was opened or filled. Source texts/images
must remain the frozen originals; do not overwrite history to fit a fixture.
Run only against the existing memory-only exchange and snapshot evidence tools.
"""

from comprehensive_cases import case, decision, trade

CASES = [
    case(
        "real_two_month_ONT_close_then_QNT_correction",
        [15257, 15258],
        [decision("close", target="A"), decision("ignore")],
        [trade("ONT", "short", 15255)],
        fixture_note="ONT exists; the later QNT correction is not a new QNT entry.",
    ),
    case(
        "real_two_month_ONT_superseded_entry_without_position",
        [15255, 15257, 15258],
        [decision("ignore"), decision("ignore"), decision("ignore")],
        fixture_note="All three messages are one backlog batch, not sequential historical fills.",
    ),
    case(
        "real_two_month_EIGEN_EDGE_conflict_two_positions",
        [15284],
        [decision("review")],
        [
            dict(trade("EIGEN", "long", 15281), entry="0.2778", tick="0.0001"),
            dict(trade("EDGE", "long", 15037), alias="B", entry="0.71", tick="0.0001"),
        ],
        fixture_note=(
            "15284 replies to EDGE text at 15283, which replies to EIGEN at 15281; "
            "also combines TP1 with undefined stop/reduction changes. Neither position may change."
        ),
    ),
    case(
        "real_two_month_PUMP_unsupported_addon_then_independent_SL",
        [15224, 15225],
        [decision("review"), decision(["sl", "targets"], target="A", sl="0.00485")],
        [dict(trade("PUMP", "long", 15221), entry="0.005198", tick="0.000001")],
        fixture_note=(
            "The add-on is unsupported, but its later reply contains an independent exact SL "
            "for the original PUMP trade. Do not add; do not discard the later SL."
        ),
    ),
    case(
        "real_two_month_PUMP_SL_without_owned_position",
        [15225],
        [decision("ignore")],
        fixture_note="The same unambiguous SL text must do nothing without an owned PUMP trade.",
    ),
    case(
        "real_two_month_FIL_close_exact_original_trade",
        [15375],
        [decision("close", target="A")],
        [dict(trade("FIL", "long", 15368), entry="0.9958", tick="0.0001")],
        fixture_note="Direct reply references the exact owned opening source 15368.",
    ),
    case(
        "real_two_month_FIL_old_reply_must_not_close_new_trade",
        [15375],
        [decision(["ignore", "review"])],
        [dict(trade("FIL", "long", 15370), entry="1.01", tick="0.0001")],
        fixture_note=(
            "15370 is ONLY a synthetic later-trade identity marker; the real post is a market "
            "digest and is not claimed to have opened FIL. Keep that original history intact. "
            "15375 explicitly replies to old source 15368, not the only active same-symbol trade."
        ),
    ),
    case(
        "real_two_month_INJ_TP1_keeps_unrelated_HYPE_unchanged",
        [15319],
        [decision("tp1", target="A")],
        [
            trade("INJ", "short", 15317),
            dict(trade("HYPE", "long", 14956), alias="B", entry="81.788", tick="0.001"),
        ],
        fixture_note="The symbol-less TP1 reply must affect INJ only, using its existing allocation.",
    ),
    case(
        "real_two_month_ETH_cancel_preserves_partial_fill_and_exits",
        [15247],
        [decision("cancel", target="A")],
        [trade("ETH", "long", 15246, limit="10", market="11", fill=4)],
        fixture_note=(
            "Synthetic price10 keeps fixture contract sizing simple; it is not a historical ETH "
            "quote or a claim that the unsupported original range order was followed. "
            "Only unfilled entry quantity is cancelled; the filled portion and its exits remain."
        ),
    ),
    case(
        "real_two_month_JUP_breakeven_uses_own_fill",
        [15274],
        [decision(["sl", "targets"], target="A", sl="0.3700")],
        [dict(trade("JUP", "short", 15272), entry="0.3700", tick="0.0001", market_after="0.3650")],
        fixture_note="Author source says0.3738; the synthetic own fill is0.3700 and must win.",
    ),
    case(
        "real_two_month_HYPE_reduce_and_breakeven_compound",
        [14960],
        [decision("review")],
        [dict(trade("HYPE", "long", 14956), entry="81.788", tick="0.001", market_after="83")],
        fixture_note=(
            "Heavy-position users should reduce once AND set cost SL. Reduction size is undefined; "
            "one decision must not silently perform only the SL half of this compound instruction."
        ),
    ),
    case(
        "real_two_month_SUI_optional_reduce_and_breakeven_compound",
        [15069],
        [decision("review")],
        [dict(trade("SUI", "long", 15065), entry="0.8192", tick="0.0001", market_after="0.8300")],
        fixture_note=(
            "Cost SL plus optional reduction for heavy positions has undefined size/selection. "
            "This strict oracle surfaces policy ambiguity, not a claim that every refusal is optimal."
        ),
    ),
    case(
        "real_two_month_JUP_three_targets_not_silently_truncated",
        [15275],
        [decision("review")],
        [dict(trade("JUP", "short", 15272), entry="0.3738", tick="0.0001")],
        fixture_note="Three source targets0.3645/0.3600/0.3555 cannot be collapsed to a two-TP plan.",
    ),
    case(
        "real_two_month_FOLKS_conditional_profit_exit_without_position",
        [15349],
        [decision("ignore")],
        fixture_note="15345 was a conditional idea; later profit/exit messages do not invent an owned trade.",
    ),
    case(
        "real_two_month_JUP_TP1_without_owned_position",
        [15362],
        [decision("ignore")],
        fixture_note="A valid TP1 reply chain still has zero effect when this follower never opened it.",
    ),
    case(
        "real_two_month_CRCL_right_side_setup_not_market_repeat_1",
        [14959],
        [decision(["ignore", "review"])],
        fixture_note=(
            "Independently inspected original chart: price sits in a descending-triangle setup; "
            "a red circle/arrow projects a move through horizontal support. No explicit current "
            "market-entry instruction, order annotation or readable entry price is supplied. "
            "The caption says bearish/right-side setup, not enter now. Automatic breakout "
            "monitoring is disabled, so conditional ignore or ambiguity review must have zero writes."
        ),
    ),
    case(
        "real_two_month_CRCL_right_side_setup_not_market_repeat_2",
        [14959],
        [decision(["ignore", "review"])],
        fixture_note=(
            "Independent identical-source repeat, with a fresh empty simulated account. "
            "The arrow visualizes a bearish scenario, not proof of an unconditional market short. "
            "Accept conditional ignore or ambiguity review, never open. No source content is changed."
        ),
    ),
]

for item in CASES:
    item["expectation_scope"] = "real_current_revision_semantics_with_constructed_owned_state"
    item["state_origin"] = "constructed_simulated_positions_not_historical_fills"

assert len(CASES) == 17
assert len({item["name"] for item in CASES}) == len(CASES)
assert all(len(item["events"]) == len(item["expected"]) for item in CASES)
