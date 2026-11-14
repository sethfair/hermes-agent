"""Candidate-only WRI-2276 fire-fenced installer; never invoked on live profile in this card.

Caller supplies byte-reviewed source and prompt, expected originals and a durable
backup manifest. This helper intentionally has no CLI/default live path.
"""
import datetime
import hashlib
import json
import os
import pathlib
import tempfile
from contextlib import contextmanager

JOB_ID = '61a6b39fba27'

class InstallRefused(RuntimeError):
    pass

def sha(data):
    return hashlib.sha256(data).hexdigest()

def unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise InstallRefused('duplicate JSON member')
        result[key] = value
    return result

def read_store(path):
    raw = pathlib.Path(path).read_bytes()
    value = json.loads(raw, object_pairs_hook=unique)
    if set(value) != {'jobs'} or not isinstance(value['jobs'], list):
        raise InstallRefused('unexpected cron store shape')
    matches = [j for j in value['jobs'] if isinstance(j, dict) and j.get('id') == JOB_ID]
    if len(matches) != 1:
        raise InstallRefused('target job not unique')
    return value, matches[0], raw

def idle(job):
    if (not job.get('enabled') or job.get('schedule', {}).get('kind') != 'interval' or
        job['schedule'].get('minutes') != 5 or job.get('script') != 'engineering_recovery_observer.py' or
        job.get('fire_claim') is not None or job.get('pending_slot') is not None or
        job.get('manual_run_at') is not None):
        raise InstallRefused('claim, slot or immutable job config changed')
    due = job.get('next_run_at')
    if not isinstance(due, str):
        raise InstallRefused('ambiguous next due')
    moment = datetime.datetime.fromisoformat(due)
    if moment.tzinfo is None or moment <= datetime.datetime.now(datetime.UTC):
        raise InstallRefused('due or ambiguous scheduler slot')

def atomic_bytes(path, data):
    path = pathlib.Path(path)
    fd, temp = tempfile.mkstemp(prefix='.wri-2276-install-', dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'wb') as out:
            out.write(data); out.flush(); os.fsync(out.fileno())
        os.replace(temp, path)
        d = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(d)
        finally: os.close(d)
        if path.read_bytes() != data:
            raise InstallRefused('write readback mismatch')
    finally:
        if os.path.exists(temp): os.unlink(temp)

def install(source_path, jobs_path, candidate_source, candidate_prompt,
            expected_source_sha, expected_prompt_sha, expected_config, jobs_module):
    """Fail closed under fire → in-process jobs → strict flock; CAS touched bytes.

    No sidecar or ledger rollback. Caller backs up independently before entering.
    On rollback CAS loss, raise mixed-version UNKNOWN for operations isolation.
    """
    source_path, jobs_path = pathlib.Path(source_path), pathlib.Path(jobs_path)
    if sha(candidate_source) == expected_source_sha or sha(candidate_prompt.encode()) == expected_prompt_sha:
        raise InstallRefused('candidate is not a change')
    with jobs_module._fire_job_lock(JOB_ID) as fenced:
        if fenced is not True:
            raise InstallRefused('fire fence unavailable')
        with jobs_module._jobs_file_lock:
            lock_fd = open(jobs_path.parent / '.jobs.lock', 'a+')
            try:
                lock_fd.seek(0)
                if jobs_module._acquire_flock(lock_fd, jobs_module._JOBS_LOCK_TIMEOUT_SECONDS) is not True:
                    raise InstallRefused('strict jobs flock unavailable')
                original_source = source_path.read_bytes()
                if sha(original_source) != expected_source_sha:
                    raise InstallRefused('source CAS mismatch')
                _store, job, old_store = read_store(jobs_path)
                if sha(job.get('prompt', '').encode()) != expected_prompt_sha:
                    raise InstallRefused('prompt CAS mismatch')
                if {k:job.get(k) for k in expected_config} != expected_config:
                    raise InstallRefused('job configuration drift')
                idle(job)
                old_prompt = job['prompt']
                try:
                    atomic_bytes(source_path, candidate_source)
                    # Strict flock excludes cooperating writers; re-read also detects
                    # a degraded/non-cooperating writer before target CAS.
                    current, current_job, current_raw = read_store(jobs_path)
                    if current_raw != old_store:
                        raise InstallRefused('jobs generation changed')
                    idle(current_job)
                    current_job['prompt'] = candidate_prompt
                    new_store = json.dumps(current, ensure_ascii=False, indent=2).encode() + b'\n'
                    atomic_bytes(jobs_path, new_store)
                    fresh, fresh_job, _ = read_store(jobs_path)
                    if fresh != current or fresh_job['prompt'] != candidate_prompt or \
                       {k:fresh_job.get(k) for k in expected_config} != expected_config or \
                       sha(source_path.read_bytes()) != sha(candidate_source):
                        raise InstallRefused('post-install readback mismatch')
                    idle(fresh_job)
                    return {'source_sha':sha(candidate_source), 'prompt_sha':sha(candidate_prompt.encode())}
                except Exception as cause:
                    # Never blanket restore the whole jobs file, or any runtime cursor.
                    _rollback(source_path, jobs_path, candidate_source, candidate_prompt,
                              original_source, old_prompt)
                    raise cause
            finally:
                jobs_module._release_flock(lock_fd)


def _rollback(source_path, jobs_path, candidate_source, candidate_prompt, original_source, old_prompt):
    try:
        _, current_job, _ = read_store(jobs_path)
        if current_job.get('prompt') == candidate_prompt:
            latest, _, _ = read_store(jobs_path)
            for item in latest['jobs']:
                if item.get('id') == JOB_ID:
                    item['prompt'] = old_prompt
            atomic_bytes(jobs_path, json.dumps(latest, ensure_ascii=False, indent=2).encode()+b'\n')
        elif current_job.get('prompt') != old_prompt:
            raise InstallRefused('prompt rollback CAS lost')
        if source_path.read_bytes() == candidate_source:
            atomic_bytes(source_path, original_source)
        elif source_path.read_bytes() != original_source:
            raise InstallRefused('source rollback CAS lost')
        if source_path.read_bytes() != original_source or \
           read_store(jobs_path)[1].get('prompt') != old_prompt:
            raise InstallRefused('rollback readback mismatch')
    except Exception as rollback:
        raise InstallRefused('MIXED VERSION UNKNOWN; isolate under authorized operator') from rollback
