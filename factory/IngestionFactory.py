from api.IConfigurator import IConfigurator
from constants import IngestionConstants
from impl.config.FileConfigurator import FileConfigurator
from impl.destination.FileDestination import FileDestination
from impl.processor.AuditColumnEnricher import AuditColumnEnricher
from impl.source.FileSource import FileSource
from impl.validate.InputValidator import InputValidator


def get_configurator(args):
    """Load the configurations."""

    if args['type'].upper() == IngestionConstants.file_type:
        return FileConfigurator()
    else:
        return IConfigurator()
    pass


def get_source(config):
    """Load the configurations."""
    return FileSource()


def get_destination(config):
    """Load the configurations."""
    return FileDestination()


def get_processor(config):
    """Load the configurations."""
    return AuditColumnEnricher()


def get_offset_tracker(config):
    """Load the configurations."""
    pass


def get_validator(config):
    """Load the configurations."""
    return InputValidator()
