"""A worker exiting first must not leave a TERM-resistant MCP child behind."""

import asyncio
import importlib.util
import os
import signal
from pathlib import Path

import pytest


async def test_exited_worker_still_reaps_its_term_resistant_process_group(tmp_path, monkeypatch):
    path = Path(__file__).parents[1] / "condor-assets/telegram_agent/worker.py"
    spec = importlib.util.spec_from_file_location("test_isolated_worker", path)
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    pid_file = tmp_path / "child.pid"
    entry = tmp_path / "exiting_worker.py"
    child_code = (
        "import os,signal,time; from pathlib import Path; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"Path({str(pid_file)!r}).write_text(str(os.getpid())); time.sleep(60)"
    )
    entry.write_text(
        "import subprocess,sys,time; from pathlib import Path\n"
        f"subprocess.Popen([sys.executable, '-c', {child_code!r}])\n"
        f"while not Path({str(pid_file)!r}).exists(): time.sleep(0.01)\n"
    )
    processes = []
    spawn = asyncio.create_subprocess_exec

    async def tracked_spawn(*args, **kwargs):
        process = await spawn(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(worker.asyncio, "create_subprocess_exec", tracked_spawn)
    task = asyncio.create_task(worker.run_isolated(entry))
    child = None
    try:
        async with asyncio.timeout(3):
            while not pid_file.exists():
                await asyncio.sleep(0.01)
            child = int(pid_file.read_text())
            await processes[0].wait()
        assert processes[0].returncode == 0
        assert os.getpgid(child) == processes[0].pid
        async with asyncio.timeout(7):
            while True:
                probe = await spawn("ps", "-p", str(child), "-o", "stat=",
                                    stdout=asyncio.subprocess.PIPE)
                status, _ = await probe.communicate()
                if not status.strip() or status.strip().startswith(b"Z"):
                    break
                await asyncio.sleep(0.05)
        assert len(processes) == 1  # no new worker before old descendants are terminated
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        for process in processes:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await process.wait()
