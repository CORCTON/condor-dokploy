"""Real-source entry-intent regressions, evaluated with untouched frozen media.

These are independently labelled current revisions, not original publication data.
No source text/image is constructed or injected. Each case starts with an empty
simulated account; the expectation is an oracle and is never sent to the model.
Positive cases deliberately distinguish colloquial/direct entry instructions from
bearish setups, including an immediate entry whose rationale mentions a breakout.
"""

from comprehensive_cases import case, decision

CASES = [
    case("real_entry_intent_CRCL_setup_not_market_1", [14959], [decision(["ignore", "review"])]),
    case("real_entry_intent_CRCL_setup_not_market_2", [14959], [decision(["ignore", "review"])]),
    case("real_entry_intent_CRCL_setup_not_market_3", [14959], [decision(["ignore", "review"])]),
    case("real_entry_intent_KMNO_colloquial_short", [15309],
         [decision("open", symbol="KMNO", side="short", price=None)]),
    case("real_entry_intent_JTO_enter_now_breakout_rationale", [15342],
         [decision("open", symbol="JTO", side="long", price=None)]),
    case("real_entry_intent_INJ_market_short", [15333],
         [decision("open", symbol="INJ", side="short", price=None)]),
    case("real_entry_intent_FIL_market_long", [15368],
         [decision("open", symbol="FIL", side="long", price=None)]),
    case("real_entry_intent_FOLKS_wait_for_breakout", [15345], [decision("ignore")]),
    case("real_entry_intent_DASH_symbol_and_chart", [15373], [decision("ignore")]),
    case("real_entry_intent_JUP_profit_screenshot", [15361], [decision("ignore")]),
]

for item in CASES:
    item["expectation_scope"] = "real_current_revision_entry_intent_not_historical_execution"
    item["fixture_note"] = (
        "Use the frozen original text and image; current visible revisions are hypothetical "
        "first arrivals. Empty owned state, no historical quotes/fills, no inferred profitability."
    )
