from api.IConfigurator import IConfigurator
from impl.config.FileConfigurator import FileConfigurator
from impl.destination.FileDestination import FileDestination
from impl.processor.AuditColumnEnricher import AuditColumnEnricher
from impl.source.FileSource import FileSource
from impl.validate.InputValidator import InputValidator
from impl.offset.FileOffsetTracker import FileOffsetTracker
import importlib
from constants import ConfigConstants as CC
from constants import IngestionConstants as IC


def get_configurator(args):
    """Load the configurations."""

    if args[CC.CONFIG_SOURCE_TYPE]:
        return get_class_instance('impl.config.' + args[CC.CONFIG_SOURCE_TYPE])
    pass


def get_source(config):
    """Load the configurations."""
    if config[CC.READ_CONFIG][CC.TYPE]:
        return get_class_instance('impl.source.' + config[CC.READ_CONFIG][CC.TYPE])


def get_destination(config):
    """Load the configurations."""
    if config[CC.WRITE_CONFIG][CC.TYPE]:
        return get_class_instance('impl.destination.' + config[CC.WRITE_CONFIG][CC.TYPE])


def get_processor(config):
    """Load the configurations."""
    return AuditColumnEnricher()


def get_offset_tracker(config):
    if config[CC.TYPE] == CC.FILE_TYPE:
        return FileOffsetTracker()


def get_validator(config):
    """Load the configurations."""
    return InputValidator()


def get_class_instance(class_name):
    module = importlib.import_module(class_name)
    print(module.__name__)
    name_array = class_name.split('.')
    cls = getattr(module, name_array[len(name_array) - 1])
    return cls()
