import argparse

from api.IConfigurator import IConfigurator
from constants import IngestionConstants
from impl.config.FileConfigurator import FileConfigurator


def get_configurator(args):
    """Load the configurations."""

    if args['type'].upper() == IngestionConstants.file_type:
        return FileConfigurator()
    else:
        return IConfigurator()
    pass


def get_source(config):
    """Load the configurations."""
    pass


def get_destination(config):
    """Load the configurations."""
    pass


def get_processor(config):
    """Load the configurations."""
    pass


def get_offset_tracker(config):
    """Load the configurations."""
    pass


def get_validator(config):
    """Load the configurations."""
    pass
