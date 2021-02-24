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
            doc['scores'] = next(iter(list(filter(lambda x: not x['advertiser']['isCompetitor'], scores))), '')
        else:
            doc['scores'] = {}
        if doc.get('solutions') and len(doc.get('solutions', [])) > 0:
            doc['solutions'] = list(map(lambda x: x['name'], doc['solutions']))
        record = self.map_record(doc)

        competitors = list(filter(lambda s: s['advertiser']['isCompetitor'], scores))
        if len(competitors) > 0:
            record['scores_competitorCount'] = len(competitors)
            record['scores_competitorScores'] = ','.join(list(map(lambda c: str(c.get('overallScore', '')), competitors)))
        record['scores_score'] = scores[0].get('overallScore', '') if len(scores) > 0 else ''

        if doc.get('estimates'):
            self.retrieve_estimates(doc, record)

        if doc.get('productSummaries'):
            self.retrieve_product_summaries(doc, record)

        if doc.get('metadata'):
            record['goals'] = ','.join(doc['metadata']['defaultProposalConfigs'].get('goals', []))
            record['originalProducts'] = ','.join(doc['metadata']['defaultProposalConfigs'].get('selectedProducts', []))

        return record

    def retrieve_estimates(self, doc, record):
        estimates = doc['estimates']
        if estimates.get('gannettDisplayEstimations') and len(estimates['gannettDisplayEstimations']) > 0:
            record['estimates_displayTactics'] = ','.join(
                map(lambda t: t['tacticName'],
                estimates['gannettDisplayEstimations'][0]['configuration'].get('tactics', [])))

        if 'rlDisplayEstimations' in estimates and len(estimates['rlDisplayEstimations']) > 0:
            record['estimates_displayTactics'] = ','.join(
                map(lambda t: t['tacticName'],
                estimates['rlDisplayEstimations'][0]['configuration'].get('tactics', [])))
        if 'socialEstimations' in estimates and len(estimates['socialEstimations']) > 0:
            record['estimates_socialEstType'] = estimates['socialEstimations'][0]['configuration'].get('configurationType', '')
            record['estimates_socialObjectives'] = estimates['socialEstimations'][0]['configuration'].get('objective', '')
        if 'videoAdsEstimations' in estimates:
            record['estimates_youtubeObjective'] = estimates['videoAdsEstimations']['configuration']['marketingObjective']
        if 'searchEstimations' in estimates and len(estimates['searchEstimations']) > 0:
            configuration = estimates['searchEstimations'][0]['configuration']
            record['search_estimates_campaignName'] = configuration.get('campaignName', '')
            record['search_estimates_website'] = configuration.get('url', '')
            record['search_estimates_categories'] = ','.join(map(lambda c: c['categoryName'], configuration.get('categories', [])))
            record['search_estimates_targetType'] = ','.join(list(set(map(lambda l: l['type'], configuration.get('locations', [])))))
            record['search_estimates_locations'] = ','.join(list(map(
                lambda x: f'{x["targetedRadius"]["radius"]} from {x["targetedRadius"].get("centerAddress", "")}'
                if x['type'] == 'RADIUS' 
                else ','.join(x['targetedLocations']),
                configuration.get('locations', []))))
            record['search_estimates_estimationType'] = configuration['estimationType']
            record['search_estimates_includeOneWordKeywords'] = configuration['includeOneWordKeywords']
            budgetEstimates = estimates['searchEstimations'][0].get('budgetEstimates')
            if budgetEstimates:
                record['search_estimates_budget'] = float(budgetEstimates['mediumSearchBudgetEstimate']['budget'])
                record['search_estimates_keywordCount'] = int(budgetEstimates['mediumSearchBudgetEstimate']['keywordCount'])
                record['search_estimates_position'] = float(budgetEstimates['mediumSearchBudgetEstimate']['averagePosition'])
                record['search_estimates_keywordTexts'] = ','.join(map(
                    lambda e: e['text'],
                    budgetEstimates['mediumSearchBudgetEstimate'].get('keywords', [])))
            record['search_estimates_productsServices'] = self.clean_text_content(','.join(configuration.get('keywordIdeas', [])))
            record['search_estimates_includeKeywordFilter'] = self.clean_text_content(','.join(configuration.get('keywordFilter', [])))
            # LOGGER.info(configuration.get('negativeKeywords', []))
            record['search_estimates_excludeKeywordFilter'] = self.clean_text_content(','.join(filter(lambda x: x, configuration.get('negativeKeywords', []) or [])))
            record['search_estimates_customKeywords'] = self.clean_text_content(','.join(map(
                lambda x: x['keyword'] if 'keyword' in x else x, 
                configuration.get('customKeywords', []))))

    def retrieve_product_summaries(self, doc, record):
        product_map = {
            'searchBudget': 'Search',
            'displayBudget': 'Display',
            'socialBudget': 'Social Ad',
            'youtubeBudget': 'Youtube',
            'emailBudget': 'Email',
            'liveChatBudget': 'Chat',
            'seoBudget': 'SEO',
            'websiteBudget': 'Website',
            'socialMediaMarketingBudget': 'Social Media',
            'videoProductionBudget': 'Video Prod',
            'listingsBudget': 'Listing',
            'clientCenterBudget': 'Client',
            'adTrackingBudget': 'Tracking',
            'fieldServiceBudget': 'Field'
        }
        for summary in doc['productSummaries']:
            for prop in product_map.items():
                if not record[prop[0]]:
                    record[prop[0]] = summary.get('productBudget') if prop[1] in summary.get('productName', '') and summary.get('productBudget') else ''


    def get_property(self, prop_name, prop_type, doc):
        nested = '_' in prop_name and prop_name != '_id'
        val = ''
        if nested:
            path_items = list(filter(lambda x: x != '', prop_name.split('_')))
            val = doc
            for item in path_items:
                val = val.get(item if item != 'id' else '_id', {}) if val else ''
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
            val = self.clean_text_content(str(val))
        return val

    def clean_text_content(self, content):
        forbidden = ['\n', '\r', '\0', '\x00']
        for char in forbidden:
            content = content.replace(char, ' ')
        return content
