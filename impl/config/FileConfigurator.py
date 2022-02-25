from api.IConfigurator import IConfigurator


class FileConfigurator(IConfigurator):
    def get_configuration(self, args):
        print(args['path'])
        print(args['config_id'])
        """Load the configurations."""
        pass
