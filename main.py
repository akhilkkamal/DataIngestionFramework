from factory import IngestionFactory


def execute_ingestion(name):
    config_list = IngestionFactory.get_configurator({"path": "<file_path>", "config_id": "1"})
    for config in config_list:
        # Create Instances
        source = IngestionFactory.get_source(config)
        validator = IngestionFactory.get_validator(config)
        processor = IngestionFactory.get_processor(config)
        destination = IngestionFactory.get_destination(config)

        # Execution
        df = source.read(config)
        df = validator.validate(df, config)
        df = processor.process(df, config)
        destination.write(df, config)
    pass


if __name__ == '__main__':
    execute_ingestion('parameters -> <path> <config_ids_comma_separated>')
