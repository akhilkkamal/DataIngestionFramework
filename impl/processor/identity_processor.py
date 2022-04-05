from api.processor import IProcessor


class IdentityProcessor(IProcessor):
    def __init__(self, context):
        self._context = context

    def process(self, *df):
        return df[0]
