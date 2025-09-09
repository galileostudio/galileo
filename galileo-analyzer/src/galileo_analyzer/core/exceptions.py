class GalileoError(Exception):
    """Base exception for Galileo analyzer"""

    pass


class AuthenticationError(GalileoError):
    """Authentication-related errors"""

    pass


class ProviderError(GalileoError):
    """Cloud provider-related errors"""

    pass


class ConfigurationError(GalileoError):
    """Configuration-related errors"""

    pass


class AnalysisError(GalileoError):
    """Analysis-related errors"""

    pass
