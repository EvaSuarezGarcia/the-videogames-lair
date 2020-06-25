import requests
import re
import time
from .config import gb_api_key

class GbApi(object):

    headers = {"user-agent": "ShynfiaBot"}
    last_request = -1

    def limit_rate(self):
        now = time.time()
        elapsed = now - self.last_request
        if elapsed < 3:
            time.sleep(3 - elapsed)
        self.last_request = time.time()        

    def search(self, query, resources=["game"], field_list=None):
        search_url = 'https://www.giantbomb.com/api/search/'
        params = {
            "api_key": gb_api_key,
            "format": "json",
            "query": query
        }

        if resources:
            resources = ','.join(resources)
            params["resources"] = resources

        if field_list:
            field_list = ','.join(field_list)
            params["field_list"] = field_list

        self.limit_rate()

        results = requests.get(search_url, params=params, headers=self.headers).json()
        results = results.get('results', [])

        return results

    def get_game(self, id, field_list=None):
        game_url = 'https://www.giantbomb.com/api/game/3030-' + str(id)
        params = {
            "api_key": gb_api_key,
            "format": "json"
        }

        if field_list:
            field_list = ','.join(field_list)
            params["field_list"] = field_list

        self.limit_rate()

        result = requests.get(game_url, params=params, headers=self.headers).json()
        result = result.get('results', {})

        # If no game is found, results will be an empty list
        if isinstance(result, list):
            result = {}

        return result

    def get_game_and_related_elements(self, id, field_list=None):
        game = self.get_game(id, field_list)
        
        return self.process_game_data(game)

    '''
        Utility function for extracting and cleaning metadata from a game and formatting 
        the game-element relationships into a list of (<game_id>, <element_id>)
    '''
    def process_game_data(self, result):
        # Discard games not having an id or a name
        if not 'id' in result or not 'name' in result:
            return None

        all_data = {}
        game = {}

        ### Retrieve metadata from game
        game['id'] = result['id']
        game['name'] = result['name']
        game['api_url'] = result['api_detail_url']
        game['site_url'] = result['site_detail_url']

        all_data['game'] = game

        # Release date
        if 'original_release_date' in result and result['original_release_date']:
            # For some reason, date also has a time, which we don't want
            # So we split first by ' ' and pick the first element (date), then split
            # by '-' to get its components
            game['release_year'], game['release_month'], game['release_day'] = \
                result['original_release_date'].split(' ')[0].split('-')
            game['estimated_date'] = False
        else:
            game['release_year'] = result['expected_release_year'] \
                if 'expected_release_year' in result else None
            game['release_month'] = result['expected_release_month'] \
                if 'expected_release_month' in result else None
            game['release_day'] = result['expected_release_day'] \
                if 'expected_release_day' in result else None
            game['estimated_date'] = True
        
        # Aliases. One string where each alias is separated by \r\n or \n, depending on 
        # how the developer was feeling like when they introduced the data :)
        # So let's just split using a regex :))
        aliases = self.split_aliases(result['aliases']) if 'aliases' in result and result['aliases'] else []
        aliases = self.list_with_game_id(game['id'], aliases)

        game['aliases'] = aliases

        # Concepts
        concepts, game_concepts = self.extract_elements_and_relationships('concepts', result, game['id'])
        game['concepts'] = game_concepts
        all_data['concepts'] = concepts

        # Developers
        developers, game_developers = self.extract_elements_and_relationships('developers', result, game['id'])
        game['developers'] = game_developers

        # Publishers
        publishers, game_publishers = self.extract_elements_and_relationships('publishers', result, game['id'])
        # Publisher's API URL is wrong
        for publisher in publishers:
            if publisher['api_detail_url']:
                publisher['api_detail_url'] = publisher['api_detail_url'].replace('publisher', 'company')
        
        game['publishers'] = game_publishers
        all_data['companies'] = developers + publishers

        # Franchises
        franchises, game_franchises = self.extract_elements_and_relationships('franchises', result, game['id'])
        game['franchises'] = game_franchises
        all_data['franchises'] = franchises

        # Genres
        genres, game_genres = self.extract_elements_and_relationships('genres', result, game['id'])
        game['genres'] = game_genres
        all_data['genres'] = genres
        
        # Game age ratings
        age_ratings, game_age_ratings = self.extract_elements_and_relationships('original_game_rating', result, game['id'])
        game['age_ratings'] = game_age_ratings
        all_data['age_ratings'] = age_ratings

        # Platforms
        platforms = result['platforms'] if 'platforms' in result and result['platforms'] else []
        platforms = self.complete_platforms_list(platforms)
        game_platforms = self.list_with_game_id(game['id'], self.extract_ids(platforms))
        game['platforms'] = game_platforms
        all_data['platforms'] = platforms

        # Themes
        themes, game_themes = self.extract_elements_and_relationships('themes', result, game['id'])
        game['themes'] = game_themes
        all_data['themes'] = themes

        return all_data

    def split_aliases(self, aliases):
        # Remove empty strings just in case, even though there shouldn't be any after regex splitting
        return list(filter(None, re.split('[\r*\n*]+', aliases)))

    def list_with_game_id(self, game_id, original_list):
        return list(map(lambda x, game_id=game_id: (game_id, x), original_list))

    def extract_ids(self, original_list):
        return map(lambda x: x['id'], original_list)

    '''
        Add missing keys to elements in list. Discard elements missing id or name
    '''
    def complete_general_elements_list(self, original_list):
        result_list = []
        for element in original_list:
            if not 'id' in element or not 'name' in element:
                continue
            if not 'api_detail_url' in element:
                element['api_detail_url'] = None
            if not 'site_detail_url' in element:
                element['site_detail_url'] = None
            result_list.append(element)

        return result_list

    '''
        Add missing keys to platforms. Discard elements missing id or name
    '''
    def complete_platforms_list(self, original_list):
        resut_list = []
        for element in original_list:
            if not 'id' in element or not 'name' in element:
                continue
            if not 'api_detail_url' in element:
                element['api_detail_url'] = None
            if not 'site_detail_url' in element:
                element['site_detail_url'] = None
            if not 'abbreviation' in element:
                element['abbreviation'] = None
            resut_list.append(element)

        return resut_list

    def extract_elements_and_relationships(self, key, data, game_id):
        elements = data[key] if key in data and data[key] else []
        elements = self.complete_general_elements_list(elements)
        game_elements = self.list_with_game_id(game_id, self.extract_ids(elements))

        return elements, game_elements