"""Own a Routine worker process independently of Condor's chat request slots."""

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

log = logging.getLogger(__name__)


async def run_isolated(path):
    path = Path(path).resolve()
    while True:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-u",
            "-c",
            "import runpy,sys; runpy.run_path(sys.argv[1], run_name='__main__')",
            str(path),
            start_new_session=True,
        )
        try:
            code = await process.wait()
            log.warning("%s worker exited: %s", path.stem, code)
        finally:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            # The leader may already have exited while an MCP descendant still
            # lives. Waiting only for the leader would skip forced group cleanup.
            deadline = asyncio.get_running_loop().time() + 5
            while True:
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    break
                if asyncio.get_running_loop().time() >= deadline:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    break
                await asyncio.sleep(0.05)
            await process.wait()
        await asyncio.sleep(5)
