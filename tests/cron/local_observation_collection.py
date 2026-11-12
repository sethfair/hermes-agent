"""Optional native-runner collection receipt; does not execute tests or fixtures."""
import hashlib
import json
from pathlib import Path


def pytest_collection_finish(session):
    target = str(session.config.getini('cache_dir'))
    nodes = sorted(item.nodeid for item in session.items)
    invocation = list(map(str, session.config.args))
    key = hashlib.sha256(json.dumps(invocation).encode()).hexdigest()
    path = Path(target)
    path.mkdir(parents=True, exist_ok=True)
    (path / (key + '.json')).write_text(json.dumps({
        'invocation': invocation, 'nodes': nodes, 'count': len(nodes),
        'collect_only': session.config.option.collectonly}, indent=2))
