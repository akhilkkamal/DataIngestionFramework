import importlib

from constants import ConfigConstants as CC
from impl.offset.FileOffsetTracker import FileOffsetTracker
from impl.processor.AuditColumnEnricher import AuditColumnEnricher



def get_configurator(args):
    """Load the configurations."""

    if args[CC.CONFIG_SOURCE_TYPE]:
        return get_class_instance('impl.config.' + args[CC.CONFIG_SOURCE_TYPE])
    pass


def get_source(context):
    """Load the configurations."""
    if context.get_config[CC.READ_CONFIG][CC.TYPE]:
        return get_class_instance('impl.source.' + context.get_config[CC.READ_CONFIG][CC.TYPE])


def get_destination(context):
    """Load the configurations."""

    if context.get_config[CC.WRITE_CONFIG][CC.TYPE]:
        return get_class_instance('impl.destination.' + context.get_config[CC.WRITE_CONFIG][CC.TYPE])


def get_audit_destination(context):
    """Load the configurations."""
    if context.get_config[CC.GENERAL_CONFIG][CC.AUDIT_CONFIG][CC.TYPE]:
        return get_class_instance('impl.audit.' + context.get_config[CC.GENERAL_CONFIG][CC.AUDIT_CONFIG][CC.TYPE])


def get_processor(context):
    """Load the configurations."""
    return AuditColumnEnricher()


def get_offset_tracker(context):
    if context.get_config[CC.TYPE] == CC.FILE_TYPE:
        return FileOffsetTracker()


def get_class_instance(class_name):
    module = importlib.import_module(class_name)
    print(module.__name__)
    name_array = class_name.split('.')
    cls = getattr(module, name_array[len(name_array) - 1])
    return cls()
