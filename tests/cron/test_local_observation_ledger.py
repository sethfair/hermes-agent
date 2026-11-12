"""Complete retained ordinary-ledger and native-shadow regression stream."""
from tests.cron import local_observation_ledger_cases as retained
from tests.cron.local_observation_component_support import native_component_scope


class LedgerRegression(retained.LedgerRegression):
    pass
