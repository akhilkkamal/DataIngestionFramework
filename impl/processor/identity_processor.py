from api.processor import IProcessor


class ExtractDateEnrichProcessor(IProcessor):
    def __init__(self, context):
        pass

    def process(self, *df):
        return df[0]
