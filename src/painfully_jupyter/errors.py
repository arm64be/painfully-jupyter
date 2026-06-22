class PainfullyJupyterError(Exception):
    """Base error for user-facing failures."""


class ConfigError(PainfullyJupyterError):
    """Configuration is missing or invalid."""


class StateError(PainfullyJupyterError):
    """Runtime state is invalid for the requested operation."""


class BrokerProtocolError(PainfullyJupyterError):
    """The broker rejected a request or returned an invalid protocol message."""


class RemoteSessionError(PainfullyJupyterError):
    """A claimed remote session cannot perform the requested operation."""


class SyncError(PainfullyJupyterError):
    """Sync planning or transfer failed."""
