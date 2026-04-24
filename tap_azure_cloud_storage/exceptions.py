from azure.core.exceptions import HttpResponseError, ServiceRequestError


class AzureError(Exception):
    """Class representing a generic Azure Storage error."""

    def __init__(self, message=None):
        super().__init__(message)
        self.message = message


class AzureBackoffError(AzureError):
    """Class representing all server errors that should trigger a backoff retry."""
    pass


class AzureInternalServerError(AzureBackoffError):
    """Class representing 500 status code."""
    pass


class AzureBadGatewayError(AzureBackoffError):
    """Class representing 502 status code."""
    pass


class AzureServiceUnavailableError(AzureBackoffError):
    """Class representing 503 status code."""
    pass


class AzureGatewayTimeoutError(AzureBackoffError):
    """Class representing 504 status code."""
    pass


class AzureRateLimitError(AzureError):
    """Class representing 429 status code."""
    pass


class AzureConnectionError(AzureBackoffError):
    """Class representing transient connection errors."""
    pass


class AzureConnectionResetError(AzureBackoffError):
    """Class representing connection reset errors."""
    pass


# Map Azure HTTP status codes to custom exception classes
STATUS_CODE_EXCEPTION_MAPPING = {
    500: {
        "raise_exception": AzureInternalServerError,
        "message": "The server encountered an unexpected condition which prevented"
                   " it from fulfilling the request."
    },
    502: {
        "raise_exception": AzureBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": AzureServiceUnavailableError,
        "message": "API service is currently unavailable."
    },
    504: {
        "raise_exception": AzureGatewayTimeoutError,
        "message": "The server did not receive a timely response from an upstream server."
    },
    429: {
        "raise_exception": AzureRateLimitError,
        "message": "The API rate limit has been exceeded."
    },
}

# Map raw connection-level exception types to custom classes
CONNECTION_EXCEPTION_MAPPING = {
    ServiceRequestError: {
        "raise_exception": AzureConnectionError,
        "message": "A connection error occurred."
    },
    ConnectionError: {
        "raise_exception": AzureConnectionError,
        "message": "A connection error occurred."
    },
    ConnectionResetError: {
        "raise_exception": AzureConnectionResetError,
        "message": "The connection was reset."
    },
}

# Tuple of raw exceptions to catch before translating
RAW_EXCEPTIONS = (HttpResponseError, ServiceRequestError, ConnectionError, ConnectionResetError)


def raise_for_error(ex):
    """Translate a raw Azure / connection exception into a custom AzureError subclass.

    For HttpResponseError, inspects the status_code and maps to a specific
    AzureBackoffError or AzureRateLimitError subclass. For unmapped 5xx codes,
    falls back to AzureBackoffError. For connection-level errors, maps to the
    appropriate AzureConnectionError subclass. Otherwise re-raises the original
    exception unchanged.
    """
    if isinstance(ex, HttpResponseError):
        status_code = ex.status_code
        if status_code in STATUS_CODE_EXCEPTION_MAPPING:
            mapping = STATUS_CODE_EXCEPTION_MAPPING[status_code]
            raise mapping["raise_exception"](f"Error: {ex}, {mapping['message']}") from ex
        # Fallback: any unmapped 5xx → AzureBackoffError
        if status_code and 500 <= status_code < 600:
            raise AzureBackoffError(str(ex)) from ex

    for raw_exc_type, mapping in CONNECTION_EXCEPTION_MAPPING.items():
        if isinstance(ex, raw_exc_type):
            raise mapping["raise_exception"](f"Error: {ex}, {mapping['message']}") from ex

    raise ex
