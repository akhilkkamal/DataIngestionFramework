from api.IConfigurator import IConfigurator
from impl.config.FileConfigurator import FileConfigurator
from impl.destination.FileDestination import FileDestination
from impl.processor.AuditColumnEnricher import AuditColumnEnricher
from impl.source.FileSource import FileSource
from impl.validate.InputValidator import InputValidator
from impl.offset.FileOffsetTracker import FileOffsetTracker

from constants import ConfigConstants as CC

def get_configurator(args):
    """Load the configurations."""

    if args[CC.CONFIG_SOURCE_TYPE].upper() == CC.CONFIG_SOURCE_FILE_TYPE:
        return FileConfigurator()
    else:
        return IConfigurator()
    pass


def get_source(config):
    """Load the configurations."""
    if config[CC.READ_TYPE] == CC.FILE_TYPE:
        return FileSource()


def get_destination(config):
    """Load the configurations."""
    if config[CC.WRITE_TYPE] == CC.FILE_TYPE:
        return FileDestination()


def get_processor(config):
    """Load the configurations."""
    return AuditColumnEnricher()


def get_offset_tracker(config):
    if config[CC.OFFSET_TYPE] == CC.FILE_TYPE:
        return FileOffsetTracker()



def get_validator(config):
    """Load the configurations."""
    return InputValidator()
