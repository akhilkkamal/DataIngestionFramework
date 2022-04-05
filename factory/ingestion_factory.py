import importlib

from constants import config_constants as CC
from context.ingestion_context import IngestionContext
from impl.offset.file_offset_tracker import FileOffsetTracker


def get_configurator(args, spark):
    return get_class_instance('impl.config.' + args[CC.CONFIG_SOURCE_TYPE])(args, spark)


def get_source(context):
    """Load the configurations."""
    read_configs = context.get_config[CC.READ_CONFIG]
    sources = [get_class_instance('impl.source.' + read_config[CC.TYPE])(context, read_config)
               for read_config in read_configs]
    return sources


def get_destination(context):
    """Load the configurations."""
    if context.get_config[CC.WRITE_CONFIG][CC.TYPE]:
        return get_class_instance('impl.destination.' + context.get_config[CC.WRITE_CONFIG][CC.TYPE])(context)


def get_audit_destination(context):
    """Load the configurations."""
    if context.get_config[CC.GENERAL_CONFIG][CC.AUDIT_CONFIG][CC.TYPE]:
        return get_class_instance('impl.audit.' +
                                  context.get_config[CC.GENERAL_CONFIG][CC.AUDIT_CONFIG][CC.TYPE])(context)


def get_processor(context: IngestionContext):
    """Load the configurations."""

    if CC.PROCESSOR_CONFIG in context.get_config:
        logger = context.get_logger
        logger.error(str(context.get_config[CC.PROCESSOR_CONFIG]))
        logger.error("impl.processor." + context.get_config[CC.PROCESSOR_CONFIG][CC.TYPE])
        return get_class_instance('impl.processor.' +
                                  context.get_config[CC.PROCESSOR_CONFIG][CC.TYPE])(context)
    else:
        return get_class_instance('impl.processor.identity_processor.IdentityProcessor')(context)


def get_offset_tracker(context):
    if context.get_config[CC.TYPE] == CC.FILE_TYPE:
        return FileOffsetTracker()


def get_class_instance(class_name):
    name_array = class_name.split('.')
    module_name = name_array[:(len(name_array) - 1)]
    module = importlib.import_module(".".join(module_name))
    print(module.__name__)
    cls = getattr(module, name_array[len(name_array) - 1])
    return cls
