class StreambusError(Exception):
    pass


class ConfigurationError(StreambusError):
    pass


class EventValidationError(StreambusError):
    pass
