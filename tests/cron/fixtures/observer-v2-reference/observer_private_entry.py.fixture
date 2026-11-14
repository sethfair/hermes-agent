"""Pinned private V2 commands; status remains an explicit component operation."""
from pathlib import Path
import stat
import sys


def _package():
    from hermes_constants import get_hermes_home
    from cron.scheduler_local_observation import load_private_package
    return load_private_package(get_hermes_home().absolute() / 'scripts' / 'observer_v2')


def _active(directory):
    path = directory / 'local-observation-v2.sqlite'
    if path.is_symlink():
        raise ValueError('symlinked V2 store')
    if path.exists() and not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError('invalid V2 store type')
    return path.is_file()


def native_prerun(directory, native_db, board, *, enforce_board=False):
    modules = _package()
    legacy = modules['engineering_recovery_observer']
    directory = Path(directory)
    if directory != legacy._checked_home() / 'cron' / ('recovery-' + legacy.JOB_ID):
        raise ValueError('unexpected recovery directory')
    if _active(directory):
        return modules['observer_native_v2'].native_prerun(
            directory, native_db, board, enforce_board=enforce_board)
    return legacy.native_prerun(directory, native_db, board, enforce_board=enforce_board)


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    modules = _package()
    legacy, v2 = modules['engineering_recovery_observer'], modules['local_observation_v2']
    home = legacy._checked_home()
    directory = home / 'cron' / ('recovery-' + legacy.JOB_ID)
    active = _active(directory)
    from hermes_cli import kanban_db
    with kanban_db.pin_first_board_resolution():
        board = kanban_db.kanban_db_path(board=legacy.BOARD)
        if not argv:
            outcome = native_prerun(directory, home / 'cron/notepad.db', board, enforce_board=True)
            print(v2.encode(outcome).decode('ascii'))
            return
        command = argv[0]
        if command == 'local-select':
            if not active or len(argv) not in (5, 6):
                raise ValueError('usage: local-select <execution-id> <epoch-id> <revision> <batch-limit> [after-root] on V2')
            result = modules['observer_native_v2'].select_current(
                directory, board, argv[1], argv[2], int(argv[3]),
                output_dir=home / 'cache/scratch', limit=int(argv[4]),
                after_root=argv[5] if len(argv) == 6 else None)
        elif command == 'local-stage':
            if not active or len(argv) != 5:
                raise ValueError('usage: local-stage <execution-id> <root> <observation-id> <revision> on V2')
            result = v2.Store(directory).stage_observation(argv[1], argv[2], argv[3], int(argv[4]), board)
        elif command in {'reconcile-status', 'ledger', 'migrate', 'resume-cutover', 'observe-local', 'local-check', 'ack'}:
            if active and command in {'observe-local', 'local-check'}:
                raise ValueError('legacy writer retired after V2 activation')
            return legacy.main(argv)
        else:
            raise ValueError('unexpected private command')
        print(v2.encode(result).decode('ascii'))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print('private observer command failed: ' + str(exc), file=sys.stderr)
        raise SystemExit(1) from exc
