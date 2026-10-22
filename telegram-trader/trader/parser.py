"""Conservative text grammar. Never interpret screenshots or forecast language as orders."""

import re
import unicodedata

from .models import Signal

_TRANSLATE = str.maketrans(
    "當價進場單倉補損獲勢壓鍵後擇這筆於", "当价进场单仓补损获势压键后择这笔于"
)
_NUMBER = r"([0-9]+(?:\.[0-9]+)?)"


def parse(text: str, reply_symbol: str | None = None) -> Signal:
    text = unicodedata.normalize("NFKC", text).translate(_TRANSLATE).strip()
    symbols = set(re.findall(r"#([A-Za-z0-9]{2,20})\b", text))
    if not symbols:
        match = re.match(r"^([A-Z0-9]{2,20})(?:USDT)?(?:\s|$)", text)
        if match and match[1] not in {"TP1", "TP2", "SL"}:
            symbols.add(match[1])
    symbols = {s.upper().removesuffix("USDT").removesuffix("_") for s in symbols}
    if len(symbols) > 1:
        return Signal(action="review", reason="multiple symbols")
    symbol = next(iter(symbols), reply_symbol)
    if re.search(r"已(?:经)?平仓|平仓完成|止盈完成|已(?:经)?止盈|复盘|回顾|昨天|昨日", text):
        return Signal(action="ignore", symbol=symbol, reason="historical/completed report")
    if re.search(
        r"(?:不要|不再|取消|暂不|不必|不用|勿|不建议).{0,20}(?:开仓|入场|进场|多单|空单|平仓|止盈|止损|TP|SL)",
        text,
        re.IGNORECASE,
    ):
        return Signal(
            action="review", symbol=symbol, reason="question/negation/cancellation, not a new order"
        )
    if re.search(r"[?？]", text) and re.search(
        r"开仓|入场|进场|平仓|止盈|止损|TP|SL", text, re.IGNORECASE
    ):
        return Signal(action="review", symbol=symbol, reason="trading question, not an instruction")
    if re.search(r"等待|突破.{0,15}后|跌破.{0,15}后|如果|若|一旦|右侧", text):
        return Signal(action="ignore", symbol=symbol, reason="conditional/watch signal")
    if re.search(r"平仓|止盈|减仓", text) and re.search(r"一半|部分|[0-9]+\s*%", text):
        return Signal(
            action="review",
            symbol=symbol,
            reason="explicit partial allocation needs a supported TP reference",
        )
    if re.search(r"全部止[盈赢]|平仓|全部出场|清仓|全部离场|止盈离场", text):
        return Signal(
            action="close" if symbol else "review",
            symbol=symbol,
            reason="explicit close" if symbol else "symbol missing",
        )
    if re.search(r"当前价.{0,12}(?:TP[12]|止盈)|(?:执行|做|平|止盈)\s*TP[12]", text, re.IGNORECASE):
        tp = re.search(r"TP([12])", text, re.IGNORECASE)
        if tp:
            return Signal(action=f"tp{tp[1]}" if symbol else "review", symbol=symbol)
    stop = re.search(
        r"(?:SL|止损)(?:\s*(?:放在|设为|设置|移到|上移至|下移至|:|到|在))*\s*" + _NUMBER,
        text,
        re.IGNORECASE,
    )
    targets = {
        "tp" + match[0]: match[1]
        for match in re.findall(
            r"TP([12])\s*(?:放在|设为|设置|移到|:|到|在)?\s*" + _NUMBER, text, re.IGNORECASE
        )
    }
    opening = re.search(r"(?:当前价(?:格)?|市价|现价).{0,10}(?:入场|进场|开仓)", text)
    if targets and not opening:
        if stop:
            targets["sl"] = stop[1]
        return Signal(action="targets" if symbol else "review", symbol=symbol, levels=targets)
    if (
        re.search(r"均价|我的开仓价|持仓均价|拿下利润|成功|收益|利润不错|已到TP|TP[12]已到", text)
        and not stop
        and not targets
    ):
        return Signal(action="ignore", symbol=symbol, reason="position report, not an instruction")
    if re.search(r"补仓|加仓", text):
        if stop:
            return Signal(
                action="sl" if symbol else "review",
                symbol=symbol,
                price=stop[1],
                reason="SL only; add-on budget not configured",
            )
        return Signal(action="review", symbol=symbol, reason="add-on budget not configured")
    if stop and not opening:
        return Signal(action="sl" if symbol else "review", symbol=symbol, price=stop[1])
    if re.search(r"均价|我的开仓价|持仓均价|拿下利润|成功|收益|利润不错", text):
        return Signal(action="ignore", symbol=symbol, reason="position report, not an instruction")
    side_match = re.search(r"(多|空)单", text)
    if side_match and re.search(r"入场|进场|开仓|做[多空]", text):
        if not symbol:
            return Signal(action="review", reason="symbol missing")
        side = "long" if side_match[1] == "多" else "short"
        if re.search(r"当前价(?:格)?|现价|市价", text):
            return Signal(
                action="open",
                symbol=symbol,
                side=side,
                levels=targets | ({"sl": stop[1]} if stop else {}),
            )
        price = re.search(r"(?:在|价格|价位|限价)\s*" + _NUMBER, text)
        if price:
            return Signal(action="open", symbol=symbol, side=side, price=price[1])
        return Signal(action="review", symbol=symbol, reason="entry type/price missing")
    if re.search(
        r"入场|进场|开仓|平仓|止损|止盈|减仓|补仓|加仓|TP[12]|\bSL\b", text, re.IGNORECASE
    ):
        return Signal(
            action="review", symbol=symbol, reason="potential instruction not safely resolved"
        )
    return Signal(action="ignore", symbol=symbol, reason="no trading instruction")
