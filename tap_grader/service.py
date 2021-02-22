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
        self.schema_map = {
            'advertiser_data': 'AdvertiserData',
            'batch_job': 'BatchJob',
            'dashboard': 'Dashboard',
            'google_my_business_data': 'GoogleMyBusinessData',
            'manage_keywords_action': 'ManageKeywordsAction',
            'marketing_attributes_data': 'MarketingAttributesData',
            'potential_lead': 'PotentialLead',
            'targeted_email_price_sheet': 'TargetedEmailPriceSheet',
            'user_profile': 'UserProfile'
        }

    def get_reports(self):
        for doc in self.client.proposal_tool[self.schema_map[self.stream]].find():
            singer.write_record(self.stream, self.map_record(doc))
    def map_record(self, doc):
        record = {}
        for prop in self.props.items():
            prop_type = prop[1]['type']
            prop_name = prop[0]
            record[prop_name] = self.get_property(prop_name, prop_type, doc)
        return record

    def get_property(self, prop_name, prop_type, doc):
        nested = '_' in prop_name and prop_name != '_id'
        val = ''
        if nested:
            path_items = list(filter(lambda x: x != '', prop_name.split('_')))
            val = doc
            for item in path_items:
                val = val.get(item if item != 'id' else '_id', {})
            val = val if val else ''
        else:
            val = doc.get(prop_name, '')

        if prop_type == 'integer':
            val = int(val) if val else ''
        elif prop_type == 'number':
            val = float(val) if val else ''
        else:
            if isinstance(val, list):
                val = ','.join(val)
            val = str(val).replace('\n', '\\n').replace('\r', '\\r').replace('\0', '').replace('\x00', '')
        return val
