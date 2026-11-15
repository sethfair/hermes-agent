"""Pure-data defaults for the Kanban configuration section."""


AGED_READY_DEFAULTS = {
    # Aged-Ready observation is metadata-only and deliberately OFF.  A
    # later deployment must supply an explicit writer roster before it can
    # become active; this setting never enables delivery.
    "dispatch_profiles": [],
    "aged_ready": {
        "observation_enabled": False,
        "controller_id": "",
        "min_age_seconds": 1800,
        "min_eligible_misses": 3,
        "controller_stale_seconds": 180,
        "owner_retry_seconds": [300, 900, 3600],
        "manager_retry_seconds": [900, 3600],
        "expected_writer_scopes": {},
        "resolver_bindings": {},
    },
}
