"""Bounded local-only unittest launcher. Production entry accepts no fixture-root override."""
from pathlib import Path
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from agent.delegation_context import kanban_path_is_fenced
from hermes_cli import kanban_db as kb

AUTHORIZED_PARENT=Path('/home/macpro/.cache/wri-2276-native-fixtures/t_caf4ddfa')
LIVE_ROOT=Path('/home/macpro/.hermes')
LINEAGE_KEYS=('HERMES_HOME','HERMES_DELEGATED_CHILD_CONTEXT','HERMES_KANBAN_DB','HERMES_KANBAN_TASK')
DEFAULT_TIMEOUT=300
NATIVE_PINS={'kanban_db.py':'35e1fb8f0aa9b2117b583879b8a224fb2ca16bc1a53021b98f6a62111fa6fa39',
             'kanban_db_connect.py':'4b6e14bd4796db77c255d8267ff2ae24cc463db25a4ee80abe3d80d83d29ccbc'}

def check_path(path):
    path=Path(path).absolute()
    if path.is_relative_to(LIVE_ROOT):
        raise ValueError('live or fenced fixture path')
    for part in (path,*path.parents):
        try: info=part.lstat()
        except FileNotFoundError: continue
        if stat.S_ISLNK(info.st_mode): raise ValueError('symlinked fixture path or ancestor')
    resolved=path.resolve()
    pin=os.environ.get('HERMES_KANBAN_DB')
    if (resolved.is_relative_to(LIVE_ROOT) or kanban_path_is_fenced(resolved) or
            (pin and resolved==Path(pin).resolve())):
        raise ValueError('live or fenced fixture path')
    return resolved

def check_board(path,invocation=None):
    """Check immediately before a real fixture connection, including tempfile fallback."""
    if invocation is None:
        raw=os.environ.get('TMPDIR')
        if not raw: raise ValueError('explicit invocation TMPDIR required')
        invocation=Path(raw)
    root=check_path(invocation)
    board=check_path(path)
    if board==root or not board.is_relative_to(root): raise ValueError('board outside invocation TMPDIR')
    return board

def _identity(path):
    info=path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.getuid():
        raise ValueError('fixture directory ownership/type mismatch')
    return info.st_dev,info.st_ino

def _active_group(pgid):
    # Linux /proc distinguishes terminated zombies (no open files) from live descendants.
    active=[]
    for p in Path('/proc').iterdir():
        if not p.name.isdigit(): continue
        try:
            fields=(p/'stat').read_text().rsplit(')',1)[1].split()
            if int(fields[2])==pgid and fields[0] not in ('Z','X'): active.append(int(p.name))
        except (FileNotFoundError,ProcessLookupError): continue
    return active

def _stop_group(proc):
    for sig in (signal.SIGTERM,signal.SIGKILL):
        try: os.killpg(proc.pid,sig)
        except ProcessLookupError: pass
        deadline=time.monotonic()+3
        while time.monotonic()<deadline:
            proc.poll()
            if not _active_group(proc.pid):
                proc.wait(timeout=3)
                return True
            time.sleep(0.02)
    return not _active_group(proc.pid)

def _qualify():
    native=Path(kb.__file__).resolve().parent
    for name,expected in NATIVE_PINS.items():
        if hashlib.sha256((native/name).read_bytes()).hexdigest()!=expected:
            raise ValueError('native module pin changed: '+name)
    if 'CREATE TABLE' not in kb.SCHEMA_SQL: raise ValueError('native schema unavailable')
    return str(native.parent),sqlite3.sqlite_version

def _run(parent,tail,timeout=DEFAULT_TIMEOUT):
    """Internal synthetic-parent seam; main below always supplies AUTHORIZED_PARENT."""
    native,sqlite_version=_qualify()
    parent=check_path(parent)
    # Missing common parent: create mode0700; never chmod or remove an existing ancestor.
    parent.parent.mkdir(mode=0o700,exist_ok=True)
    check_path(parent)
    parent.mkdir(mode=0o700)  # exclusive; existing parents are never adopted
    parent_id=_identity(parent)
    invocation=None; invocation_id=None; proc=None; safe=True; result=1
    try:
        invocation=Path(tempfile.mkdtemp(prefix='invocation-',dir=parent))
        invocation_id=_identity(invocation)
        check_path(invocation)
        env=dict(os.environ)
        env.update(TMPDIR=str(invocation),PYTHONDONTWRITEBYTECODE='1',
                   PYTHONPATH=str(Path(__file__).resolve().parent)+os.pathsep+native)
        print(json.dumps({'fixture_invocation':str(invocation),'python':sys.executable,
                          'sqlite':sqlite_version,'timeout_seconds':timeout,
                          'lineage':{k:env.get(k) for k in LINEAGE_KEYS}}),flush=True)
        proc=subprocess.Popen([sys.executable,'-B','-m','unittest',*tail],
                              cwd=Path(__file__).resolve().parent,env=env,
                              start_new_session=True)
        try: result=proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            print('fixture child timeout; terminating owned process group',flush=True)
            safe=_stop_group(proc); result=124
        if _active_group(proc.pid):
            safe=_stop_group(proc)
            result=result or 1  # a leaked worker disqualifies an otherwise green child
    finally:
        if proc is not None and proc.poll() is None:
            safe=_stop_group(proc) and safe
        try:
            if not safe: raise RuntimeError('surviving child process; cleanup UNKNOWN')
            check_path(parent)
            if _identity(parent)!=parent_id: raise RuntimeError('task parent identity changed')
            if invocation is not None:
                check_path(invocation)
                if _identity(invocation)!=invocation_id: raise RuntimeError('invocation identity changed')
                shutil.rmtree(invocation)
            parent.rmdir()  # refuse unexpected siblings, never recursively clear task/common parent
            print(json.dumps({'cleanup':'removed','task_parent':str(parent),
                              'invocation':str(invocation)}),flush=True)
        except Exception as exc:
            print(json.dumps({'cleanup':'UNKNOWN','task_parent':str(parent),
                              'invocation':str(invocation),'error':str(exc)}),flush=True)
            result=1
    return result

def main(argv=None):
    tail=list(sys.argv[1:] if argv is None else argv)
    if not tail: tail=['local_observation_fixture_child.ChildTests']
    try: return _run(AUTHORIZED_PARENT,tail)
    except Exception as exc:
        print('fixture PRETEST: '+str(exc),file=sys.stderr,flush=True)
        return 1

if __name__=='__main__': sys.exit(main())
