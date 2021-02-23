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
            'user_profile': 'UserProfile',
            'silktide_data': 'SilktideData',
            'proposal': 'Proposal'
        }

    def get_reports(self):
        for doc in self.client.proposal_tool[self.schema_map[self.stream]].find():
            record = self.map_proposal_record(doc) if self.stream == 'proposal' else self.map_record(doc)
            singer.write_record(self.stream, record)

    def map_record(self, doc):
        record = {}
        for prop in self.props.items():
            prop_type = prop[1]['type']
            prop_name = prop[0]
            record[prop_name] = self.get_property(prop_name, prop_type, doc)
        return record

    def map_proposal_record(self, doc):
        scores = doc.get('scores', [])
        if len(scores) > 0:
            doc['scores'] = list(filter(lambda x: not x['advertiser']['isCompetitor'], scores))[0]
        else:
            doc['scores'] = {}
        if doc.get('solutions') and len(doc.get('solutions', [])) > 0:
            doc['solutions'] = list(map(lambda x: x['name'], doc['solutions']))
        record = self.map_record(doc)

        # custom mapping logic here

        return record

    def get_property(self, prop_name, prop_type, doc):
        nested = '_' in prop_name and prop_name != '_id'
        val = ''
        if nested:
            path_items = list(filter(lambda x: x != '', prop_name.split('_')))
            val = doc
            for item in path_items:
                val = val.get(item if item != 'id' else '_id', {}) if val else val
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
