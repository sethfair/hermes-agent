"""Complete retained V1 issuance regressions, separate from private V2 admission."""
from tests.cron import local_observation_v1_cases as retained
from tests.cron.local_observation_component_support import native_component_scope


class IssuedTests(retained.IssuedTests):
    pass
