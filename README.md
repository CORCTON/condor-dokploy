# Condor on Dokploy

Deployment-only wrapper for official Condor plus the shared routines maintained
in `https://github.com/CORCTON/condor`.

Every build clones the latest official `hummingbot/condor` main branch, then
copies only four shared market-research routines from the fork. This repository
owns the Docker build, persistent-state migration, and Dokploy Compose topology so
deployment concerns do not modify Condor source.

The official stock Agent library remains read-only at `/app/agents`; Condor's
supported stock/local layering writes this installation's model overrides,
strategies, memories, and sessions to `/state/agents`. The main Agent and its
official fallback use the configured OpenAI-compatible provider.

The dedicated `hummingbot-init` image copies the current official Hummingbot
API bot assets into persistent storage, enables only the official
`gate_io_paper_trade` connector, and starts its paper account with 100 USDT. No
Gate.io credentials are required for that stock spot-paper connector. The
separate Telegram perpetual execution extension below has its own ledger.

# CPA provider

The official `eceasy/cli-proxy-api:v8.0.38` image runs without source changes.
It has no published ports; Condor uses `http://cpa:8317/v1` on `condor-internal`.
The version is explicit for reproducibility and can be upgraded deliberately.

Provision `/var/lib/cliproxy-api/config/config.yaml` from `cpa/config.example.yaml`
with independently generated client and management keys; keep the directory and
file private. OAuth records persist in `cpa-auths`. The management API is available
only on the Docker network and always needs its separate key. The panel is disabled.

Set `LLM_BASE_URL=http://cpa:8317/v1`, `LLM_API_KEY=<CPA client key>`, and
`TRADER_AGENT_MODEL=custom:devin/swe-2(high)` in Dokploy. Re-run `telegram-trader-assets`
and recreate Condor after changing that model. CPA's native reasoning suffix maps
this model to Devin SWE-2 High; do not substitute a guessed model alias.

Use CPA's `--devin-login --no-browser --config /config/config.yaml` login flow.
It produces a PKCE authorization URL and accepts the authorization code, without
requiring a public callback or management port. Authorize before enabling
`TRADER_LLM_READY`; process health alone does not imply usable upstream credentials.
Rotating the client key requires updating both CPA's access keys and Condor's
`LLM_API_KEY`. Never commit the populated config or OAuth records.

# Telegram channel execution extension

The deployment adds `telegram-trader` and one local `telegram_agent` with two
Routines. Upstream Condor files are not modified. The old Windsurf endpoint is
not used by this extension. All services/data that already exist are retained.

The entry path is Telethon update → durable SQLite event → deterministic text
parser → Gate REST order. It does **not** wait for the LLM, Web UI or a Condor
conversation lock. Condor's official continuous Routine, Agent context and LLM
client factory fill exit parameters asynchronously after an actual fill. This
background client mounts no MCP servers/tools; only its validated JSON plan can
reach the execution service. The admin's
Telegram chat is bound to TelegramAgent on startup.

## Safety and execution contract

- Initial margin budget: 20% × (classic USDT futures balance + unrealised PnL).
  Fixed isolated 7x. Contract counts round down; market slippage and fees mean
  the actual margin debit need not exactly equal that budget.
- Only explicit text market/limit entries; no breakout monitoring or add-ons.
  Same-symbol active positions are not merged. Use a dedicated, one-way,
  classic USDT futures account (`margin_mode=0`, `position_mode=single`).
  Decimal contracts and other account modes are rejected. The deprecated
  `enable_evolved_classic` field is not used to infer account mode. Leverage is
  set explicitly with `set_leverage` and `margin_mode=isolated`, then verified
  using the returned position's `pos_margin_mode`, `mode`, and `lever` fields.
- No initial stop is invented. No timed position close. AI plans are schema-,
  version-, direction- and tick-checked. Channel numeric levels take precedence.
- Native reduce-only TP limits and full-position mark-price SL triggers.
  Trigger expiry is renewed before expiry; it is **not** a position timeout.
- Acknowledgement is not a fill. Partial fills/reductions are reconciled.
  Remaining entry orders are cancelled when exits start. Every exchange write
  has a durable intent; ambiguous timeouts pause new entries and are never
  blindly resent, even when a text-ID lookup returns 404.
- Non-actionable reports/analysis are ignored. Potential instructions that
  cannot be safely interpreted are recorded as `review` and notify the admin.
  Image-only levels/symbols, ambiguous references and unconfigured partial
  allocations are not guessed. Initial support is tested text grammar, not a
  claim of understanding arbitrary natural language with 100% certainty.
- Channel messages older than 60 seconds and queued/replayed entries after a
  restart are not auto-opened. This freshness guard is separate from position
  holding time. Offline missed instructions require review; no history catch-up
  opens. The dedicated account must not be concurrently traded by other bots.
- Private API port is internal only, token-authenticated, with no entry or live
  activation endpoint. Exchange keys exist only in the execution container.
  Only Condor polls the existing Telegram bot; the service only sends notices.

## Configuration (all live trading disabled by default)

`TRADER_ENABLED=false`, `TRADER_LLM_READY=false`, `GATE_ENVIRONMENT=testnet` are
the deployment defaults. Readiness is separate from process health. Initial
activation requires deployment configuration and clearing the persisted pause
after testnet acceptance; restart preserves operator pause state.

Required from the operator:

1. Telegram `TG_API_ID`, `TG_API_HASH`, an authorized **user** Telethon session
   mounted at `/state/telegram.session`, plus the exact `TG_CHANNEL_ID` and
   optional public `TG_CHANNEL` username. A bot token cannot read a subscribed
   channel that has not added the bot. Never commit or paste the session publicly.
2. Gate USDT perpetual API key/secret, testnet/live choice, supported account mode,
   and confirmation that this is a dedicated account. No withdrawal permission.
3. An OpenAI-compatible provider's `LLM_BASE_URL`, `LLM_API_KEY` and
   `TRADER_AGENT_MODEL=custom:<model-id>`. Confirm it supports tool calling.

Existing `TELEGRAM_TOKEN` / admin ID are reused. A random internal `TRADER_TOKEN`
is generated during deployment; this is not a trading credential. Do not enable
live entry before authenticated account checks, testnet entry/partial-fill/SL/TP/
close tests and a measured end-to-end latency test. Unit tests cannot establish
real exchange behavior or promise a seconds-level production latency.

## Verification

```sh
cd telegram-trader
uv sync --frozen
uv run pytest -q
uv run ruff check trader tests condor-assets ../telegram_bootstrap.py
```

The deployment mounts only wrapper code and local Agent assets. The installer
does not delete other agents, strategy data or databases. A deployment backup
must be taken before replacing Compose/environment or the Condor wrapper.
