class NotSupported(Exception):
    """Raised when a requested operation is not supported by the provider."""
    pass

class ProviderError(Exception):
    """Raised when the provider encounters an error during data fetching."""
    pass

class DataNotFound(Exception):
    """Raised when the requested data is not found for a given symbol."""
    pass