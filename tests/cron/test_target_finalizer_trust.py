"""WRI-2276: an opt-in finalizer cannot execute a mutable local script."""

import hashlib
import json

import pytest

from cron.scheduler_local_observation import _finalize_with_observer

pytest_plugins = ("tests.cron.fixtures.wri2276_store_fixture",)


def test_world_writable_finalizer_refused_before_execution(tmp_path, monkeypatch, real_v2):
    home = tmp_path / "profile"
    scripts = home / "scripts"
    scripts.mkdir(parents=True, mode=0o700)
    marker = tmp_path / "executed"
    script = scripts / "finalizer.py"
    script.write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).touch()\n"
        "print('{\"state\":\"bad\",\"digest\":\"" + "a" * 64 +
        "\",\"execution_id\":\"synthetic-execution\",\"epoch_id\":\"" + "a" * 64 + "\"}')\n"
    )
    script.chmod(0o777)
    (home / "config.yaml").write_text(json.dumps({"cron": {
        "local_observation_finalizer_script": str(script),
        "local_observation_finalizer_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
    }}))
    monkeypatch.setenv("HERMES_HOME", str(home))
    with pytest.raises(RuntimeError):
        _finalize_with_observer({"id": "synthetic"}, {
            "execution_id": "synthetic-execution", "epoch_id": "a" * 64,
        }, object())
    assert not marker.exists(), "untrusted mutable finalizer ran before the store readback failed"


def test_symlinked_script_ancestor_refused_before_execution(tmp_path, monkeypatch, real_v2):
    home = tmp_path / "profile"
    real = home / "scripts" / "real"
    real.mkdir(parents=True, mode=0o700)
    alias = home / "scripts" / "alias"
    alias.symlink_to(real, target_is_directory=True)
    marker = tmp_path / "executed"
    target = real / "finalizer.py"
    target.write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n"
                      "print('bad output')\n")
    target.chmod(0o700)
    (home / "config.yaml").write_text(json.dumps({"cron": {
        "local_observation_finalizer_script": str(alias / "finalizer.py"),
        "local_observation_finalizer_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
    }}))
    monkeypatch.setenv("HERMES_HOME", str(home))
    with pytest.raises((RuntimeError, ValueError)):
        _finalize_with_observer({"id": "synthetic"}, {"execution_id": "id", "epoch_id": "a" * 64},
                                object())
    assert not marker.exists(), "symlinked ancestor redirected the script before refusal"
