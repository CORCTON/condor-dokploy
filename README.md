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
official fallback use WindsurfAPI's `swe-2-high` model.

The dedicated `hummingbot-init` image copies the current official Hummingbot
API bot assets into persistent storage, enables only the official
`gate_io_paper_trade` connector, and starts its paper account with 100 USDT. No
Gate.io credentials are required and no custom execution ledger is installed.
