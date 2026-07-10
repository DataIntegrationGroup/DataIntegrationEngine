class USGSRateLimitError(Exception):
    pass


class PartialOrNoDataError(Exception):
    pass


class ConfigError(Exception):
    """Invalid configuration (bad bbox/county/date/parameter). Raised by
    Config.validate() so callers decide how to fail — the CLI turns it into a
    clean exit(2), and a Dagster asset catches it and soft-fails that source
    instead of the whole run's process dying (which sys.exit() would cause)."""
    pass