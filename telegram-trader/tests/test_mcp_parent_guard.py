"""MCP's detached process group must not outlive its exact worker parent."""

import asyncio
import contextlib
import ctypes
import errno
import importlib.util
import json
import os
import runpy
import signal
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

AGENT = Path(__file__).parents[1] / "condor-assets/telegram_agent"
spec = importlib.util.spec_from_file_location("parent_guard_worker", AGENT / "worker.py")
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)


@pytest.mark.parametrize("expected", [1, 321])
def test_linux_guard_accepts_exact_parent_including_pid_one(monkeypatch, expected):
    calls = []

    def prctl(*args):
        calls.append(args)
        return 0

    monkeypatch.setattr(worker.sys, "platform", "linux")
    monkeypatch.setattr(worker.os, "getppid", lambda: expected)
    monkeypatch.setattr(ctypes, "CDLL", lambda *a, **k: SimpleNamespace(prctl=prctl))
    worker.bind_mcp_parent(expected)
    assert calls == [(1, signal.SIGTERM, 0, 0, 0)]


@pytest.mark.parametrize("parents", [(1,), (321, 1)])
def test_linux_guard_exits_if_parent_died_before_or_during_registration(monkeypatch, parents):
    values, calls = iter(parents), []

    def prctl(*args):
        calls.append(args)
        return 0

    monkeypatch.setattr(worker.sys, "platform", "linux")
    monkeypatch.setattr(worker.os, "getppid", lambda: next(values))
    monkeypatch.setattr(ctypes, "CDLL", lambda *a, **k: SimpleNamespace(prctl=prctl))
    with pytest.raises(SystemExit) as stopped:
        worker.bind_mcp_parent(321)
    assert stopped.value.code == 0
    assert len(calls) == len(parents) - 1


def test_linux_guard_fails_closed_when_kernel_registration_fails(monkeypatch):
    def prctl(*args):
        return -1

    monkeypatch.setattr(worker.sys, "platform", "linux")
    monkeypatch.setattr(worker.os, "getppid", lambda: 321)
    monkeypatch.setattr(ctypes, "CDLL", lambda *a, **k: SimpleNamespace(prctl=prctl))
    monkeypatch.setattr(ctypes, "get_errno", lambda: errno.EPERM)
    with pytest.raises(OSError) as failure:
        worker.bind_mcp_parent(321)
    assert failure.value.errno == errno.EPERM


def test_linux_guard_rejects_invalid_expected_pid(monkeypatch):
    monkeypatch.setattr(worker.sys, "platform", "linux")
    with pytest.raises(ValueError, match="parent PID"):
        worker.bind_mcp_parent(0)


def test_non_linux_guard_does_not_call_prctl(monkeypatch):
    monkeypatch.setattr(worker.sys, "platform", "darwin")
    monkeypatch.setattr(ctypes, "CDLL", lambda *a, **k: pytest.fail("Linux-only API"))
    worker.bind_mcp_parent(321)


def test_channel_tools_exits_orphan_before_importing_mcp_dependencies(monkeypatch):
    monkeypatch.setenv("TRADER_PARENT_PID", "321")
    monkeypatch.setattr(worker.sys, "platform", "linux")
    monkeypatch.setattr(worker.os, "getppid", lambda: 1)
    monkeypatch.setitem(sys.modules, "worker", worker)
    monkeypatch.setitem(sys.modules, "aiohttp", None)
    with pytest.raises(SystemExit) as stopped:
        runpy.run_path(str(AGENT / "channel_tools.py"), run_name="__main__")
    assert stopped.value.code == 0


@pytest.mark.skipif(sys.platform != "linux", reason="Linux PR_SET_PDEATHSIG integration")
async def test_detached_child_dies_when_worker_parent_exits():
    child_code = (
        "import json,os,sys,time; "
        f"sys.path.insert(0,{str(AGENT)!r}); "
        "from worker import bind_mcp_parent; "
        "bind_mcp_parent(int(sys.argv[1])); "
        "print(json.dumps({'pid':os.getpid(),'pgid':os.getpgrp(),'ppid':os.getppid()}),flush=True); "
        "time.sleep(30)"
    )
    parent_code = (
        "import os,subprocess,sys; "
        f"subprocess.Popen([sys.executable,'-u','-c',{child_code!r},str(os.getpid())], "
        "start_new_session=True, stdin=subprocess.DEVNULL); "
        "sys.stdin.readline()"
    )
    parent = await asyncio.create_subprocess_exec(
        sys.executable, "-u", "-c", parent_code, start_new_session=True,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
    )
    child = None
    try:
        async with asyncio.timeout(5):
            metadata = json.loads(await parent.stdout.readline())
        child = metadata["pid"]
        assert metadata["ppid"] == parent.pid
        assert metadata["pgid"] == child and child != parent.pid
        os.kill(child, 0)
        parent.terminate()
        async with asyncio.timeout(3):
            await parent.wait()
            while True:
                try:
                    status = Path(f"/proc/{child}/stat").read_text().rsplit(")", 1)[1].split()[0]
                except FileNotFoundError:
                    break
                if status == "Z":  # exited; container init may not have reaped it yet
                    break
                await asyncio.sleep(0.02)
    finally:
        if child is not None:
            with contextlib.suppress(ProcessLookupError):
                os.kill(child, signal.SIGKILL)
        if parent.returncode is None:
            parent.kill()
        await parent.wait()
