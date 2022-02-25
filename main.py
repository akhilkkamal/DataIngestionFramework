
from factory import IngestionFactory
import argparse


def execute_ingestion(args):
    configurator = IngestionFactory.get_configurator(args)
    configurator.get_configuration(args)
    # for config in config_list.get_configuration():
    #     # Create Instances
    #     source = IngestionFactory.get_source(config)
    #     validator = IngestionFactory.get_validator(config)
    #     processor = IngestionFactory.get_processor(config)
    #     destination = IngestionFactory.get_destination(config)
    #
    #     # Execution
    #     df = source.read(config)
    #     df = validator.validate(df, config)
    #     df = processor.process(df, config)
    #     destination.write(df, config)
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("type",
                        help="configuration input type")
    parser.add_argument("path",
                        help="path of the file")
    parser.add_argument("config_id",
                        help="config ids to be ingested")
    args = parser.parse_args().__dict__
    print(args)
    execute_ingestion(args)
