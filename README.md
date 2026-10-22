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
`TRADER_AGENT_MODEL=custom:devin/gemini-4-argon(medium)` in Dokploy. CPA's native
reasoning suffix selects Gemini 4 Medium. When changing models, synchronize local
Agent configs, user preferences, conversation resume metadata and Telegram's saved
selection through Condor's supported stores/APIs; changing the environment alone
does not replace persisted choices. Re-run `telegram-trader-assets` and recreate
Condor after synchronizing these selections. Never substitute a guessed model alias.

Use CPA's `--devin-login --no-browser --config /config/config.yaml` login flow.
It produces a PKCE authorization URL and accepts the authorization code, without
requiring a public callback or management port. Authorize before enabling
`TRADER_LLM_READY`; process health alone does not imply usable upstream credentials.
Rotating the client key requires updating both CPA's access keys and Condor's
`LLM_API_KEY`. Never commit the populated config or OAuth records.

# Telegram channel execution extension

The deployment adds `telegram-trader` and one local `telegram_agent` with three
Routines. Upstream Condor files are not modified. The old Windsurf endpoint is
not used by this extension. All services/data that already exist are retained.

Every source-channel update (including chatter, edits and media) enters a durable
SQLite inbox. `signal_interpreter` uses Condor's official model-client factory and
the configured Gemini model to interpret each message. It receives current images
and can call four read-only MCP tools: full paginated channel history/search,
individual messages/reply links, original images, and owned execution state.
There is no keyword parser, semantic prefilter or fallback entry path in the service.
The legacy parser is retained **only as canned executor test data**, not model-accuracy evidence.

The model calls `submit_signal_decisions` with typed arguments associated with exact
event and owned-trade IDs; free-text JSON is never parsed as a channel decision.
The tool stages a validated proposal only. After the model completes, the supervisor
rechecks freshness before execution. MCP schema errors are returned to the model
as tool errors; invalid arguments never become orders.
The service validates them before Gate REST execution. A new message or changed
trade state invalidates an in-flight analysis; all unhandled messages are interpreted
together again. A timed-out model never authorizes a guessed order. History reads
never enter the execution queue. Channel context tools cannot send messages, trade,
access another channel or retrieve secrets. Images have an explicit 8 MiB transport
limit; message text is never silently truncated. Analysis batches are paginated at
50 events/four media items; unread backlog blocks new entries, not order reconciliation.

`exit_planner` fills TP/SL parameters asynchronously after a confirmed fill. Its
only tool, `submit_exit_plan`, stages typed parameters for the assigned trade,
version and lease. It has no order, account, history or arbitrary network tools.
Free-text JSON is not parsed as a plan. The same direction/tick/lease checks run
before staging and again before execution; invalid tool arguments can be corrected
by the model without sending exchange orders.
The admin's Telegram chat is bound to TelegramAgent on startup, and both continuous
Routines are bootstrapped through the official Routine API. Chat history and source
channel history are distinct; no ability to query history should be assumed outside
the signal interpreter's explicitly mounted read-only tools.
The signal Routine supervises its own worker process: Condor's per-process API
semaphore must not let an exit-plan or chat request block an urgent channel update.
Stopping the Routine terminates that worker and its MCP process group.

## Safety and execution contract

- Initial margin budget: 20% × (classic USDT futures balance + unrealised PnL).
  Fixed isolated 7x. Contract counts round down; market slippage and fees mean
  the actual margin debit need not exactly equal that budget.
- Only model-interpreted explicit unconditional market/limit entries; no automatic
  channel breakout monitoring or add-ons. Manual chart-based conditional trading
  is not enabled by this extension and must not share ownership of these positions.
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
- Gemini classifies non-actionable reports/analysis as `ignore`. Ambiguous
  instructions become `review` and notify the admin. Contextual phrases such as
  平掉/离场/撤掉 are interpreted with history and actual owned-trade state, not a
  dictionary. `cancel` cancels only unfilled entry quantity; it does not remove
  protective orders or flatten filled positions. `close` cancels the remaining
  entry then flattens the owned position. Unreadable charts, unresolved references
  and undefined partial allocations are not guessed. Model accuracy is not 100%.
- Channel messages older than 60 seconds and queued/replayed entries after a
  restart are still interpreted but are not auto-opened. This freshness guard is separate from position
  holding time. Offline missed instructions require review; no history catch-up
  opens. The dedicated account must not be concurrently traded by other bots.
- Private API port is internal only, token-authenticated, with no entry or live
  activation endpoint. A decision submission requires a current durable analysis
  lease and complete event coverage; it is not a free-form order endpoint.
  Exchange keys are never exposed through model tools or status responses.
  Only Condor polls the existing Telegram bot; the service only sends notices.

## Configuration (all live trading disabled by default)

`TRADER_ENABLED=false`, `TRADER_LLM_READY=false`, `GATE_ENVIRONMENT=testnet` are
the deployment defaults. Readiness is separate from process health. Initial
activation requires deployment configuration and clearing the persisted pause
after testnet acceptance; restart preserves operator pause state.
Telegram collection and interpretation can run with exchange execution disabled;
they do not require exchange keys. New decisions remain non-executable while disabled.

Required from the operator:

1. Telegram `TG_API_ID`, `TG_API_HASH`, an authorized **user** Telethon session
   mounted at `/state/telegram.session`, plus the exact `TG_CHANNEL_ID` and
   optional public `TG_CHANNEL` username. A bot token cannot read a subscribed
   channel that has not added the bot. Never commit or paste the session publicly.
2. An existing Hummingbot `gate_io_perpetual` credential, testnet/live choice,
   supported account mode, and confirmation that this is a dedicated account.
   No withdrawal permission. See credential reuse below; no second live key is needed.
3. An OpenAI-compatible provider's `LLM_BASE_URL`, `LLM_API_KEY` and
   `TRADER_AGENT_MODEL=custom:<model-id>`. Confirm it supports tool calling.

Existing `TELEGRAM_TOKEN` / admin ID are reused. A random internal `TRADER_TOKEN`
is generated during deployment; this is not a trading credential. Do not enable
live entry before authenticated account checks, testnet entry/partial-fill/SL/TP/
close tests and a measured end-to-end latency test. Unit tests cannot establish
real exchange behavior or promise a seconds-level production latency.
The previous sub-second parser-to-order measurements do **not** apply to this
model-first architecture. Measure actual channel-to-model-to-exchange latency anew.

### Credential reuse

Compose defaults to `GATE_CREDENTIAL_SOURCE=hummingbot`. It read-only mounts
the selected account's connector directory (`HUMMINGBOT_ACCOUNT_NAME=master_account`
by default), not the full Hummingbot bots volume or Docker socket. The service reads
only `gate_io_perpetual.yml`, using Hummingbot's V3 keyfile decoding library and the
existing `HUMMINGBOT_CONFIG_PASSWORD`. Decrypted keys remain in process memory;
they are not copied into Dokploy environment variables, Condor, SQLite or model context.
This gives the execution container access to the selected account's encrypted connector
files and the shared decryption password; do not mount other accounts there.

Credentials are loaded at startup. After changing/removing them in Hummingbot,
restart `telegram-trader` to reload them (an exchange-side revocation takes effect
independently). Loading does not enable execution or clear the persisted pause.
Missing/invalid credentials block private Gate requests but do not stop channel analysis.
`/status` reports only source, target environment, readiness and a sanitized error.

The saved `gate_io_perpetual` connector is **live**. With `GATE_ENVIRONMENT=testnet`,
it is deliberately rejected rather than silently changing endpoints. For isolated
tests, explicitly select `GATE_CREDENTIAL_SOURCE=env` and supply testnet-only
`GATE_API_KEY` / `GATE_API_SECRET`. Mixing both sources is rejected; there is no
fallback to another account. Deployment defaults remain testnet and disabled.

The existing Telegram bot/admin ID, CPA provider/model client and Hummingbot
encryption password already have single Compose configuration sources. The
Telethon user session (channel-reader login) and execution ledger remain separate:
they are not interchangeable with a bot token or Hummingbot's order ownership.

## Verification

```sh
cd telegram-trader
uv sync --frozen
uv run pytest -q
uv run ruff check trader tests condor-assets ../telegram_bootstrap.py
```

`tests/eval_channel_model.py` is an opt-in real-provider evaluation, run in a Condor
environment with the configured Gemini client. It uses memory-only fake channel
and position evidence, stages proposals but never executes them, and asserts zero
exchange intents.
Unit tests and scripted model decisions must not be presented as real-model accuracy.
`tests/acceptance_channel_testnet.py` is a separate opt-in acceptance runner. It
uses synthetic messages, actual Gemini and actual Gate TestNet orders, verifies
cleanup of only its own positions/orders, and never uses a production Telegram
session or state database. It does not measure Telegram delivery latency.

The deployment mounts only wrapper code and local Agent assets. The installer
does not delete other agents, strategy data or databases. A deployment backup
must be taken before replacing Compose/environment or the Condor wrapper.
