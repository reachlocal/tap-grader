import singer
from pymongo import MongoClient


LOGGER = singer.get_logger()

class GraderReportingService:

    def __init__(self, stream, schema, config):
        self.config = config
        self.stream = stream
        self.props = schema["properties"]
        host = config['mongoHost']
        user = config['mongoUser']
        password = config['mongoPassword']
        port = config['mongoPort']
        authSource = config['mongoAuthSource']
        conn_string = f'mongodb://{user}:{password}@{host}:{port}/?authSource={authSource}'
        self.client = MongoClient(conn_string)

    
    def get_reports(self):
        for doc in self.client.proposal_tool['AdvertiserData'].find():
            singer.write_record(self.stream, self.map_record(doc))
    def map_record(self, doc):
        record = {}
        for prop in self.props.items():
            prop_type = prop[1]['type']
            prop_name = prop[0]
            val = ''
            if prop_type == 'integer':
                val = int(doc[prop_name]) if doc.get(prop_name) else ''
            elif prop_type == 'number':
                val = float(doc[prop_name]) if doc.get(prop_name) else ''
            else:
                val = str(doc.get(prop_name, ''))
            record[prop_name] = val
        return record