#%%
import os
import requests
import json
import gzip
import pandas as pd
# from sqlalchemy import text
import dateparser
import datetime
import time
# import utils.misc_utils as misc
from src.tmdb_data_retriever.utils import misc_utils as misc

#%%

def cleanse_title_df(df):

    # df = df.applymap(lambda x: x.replace('\r\n', ' | ') if isinstance(x, str) else x)
    # df = df.applymap(lambda x: x.replace('\r', ' | ') if isinstance(x, str) else x)
    # df = df.applymap(lambda x: x.replace('\n', ' | ') if isinstance(x, str) else x)
    # df = df.applymap(lambda x: x.replace('\t', ' ') if isinstance(x, str) else x)
    # df = df.applymap(lambda x: x.replace('  ', ' ') if isinstance(x, str) else x)
    df = df.applymap(misc.cleanse_value)
    df = df.rename(columns={'id': 'tmdb_id', 'title': 'title_name'})

    return df

class MovieData:
    def __init__(
            self, 
            my_settings, 
            # api_key, 
            local_db, 
            api_response, 
            # output_path, 
            # output_titles_flag=True, 
            # output_title_genres_flag=True, 
            # output_genres_flag=True, 
            # output_title_spoken_languages_flag=True, 
            # output_spoken_languages_flag=True, 
            # output_title_production_countries_flag=True, 
            # output_production_countries_flag=True, 
            # output_title_production_companies_flag=True, 
            # output_production_companies_flag=True, 
            # output_title_collections_flag=True, 
            # output_collections_flag=True, 
            # output_title_keywords_flag=True, 
            # output_keywords_flag=True, 
            # output_title_removed_flag=True
            ):

        self.my_settings = my_settings
        self.api_key = self.my_settings.api_key
        self.local_db = local_db
        self.api_response = api_response
        self.output_path = self.my_settings.output_path
        self.output_titles_flag = self.my_settings.output_titles_flag
        self.output_title_genres_flag = self.my_settings.output_title_genres_flag
        self.output_genres_flag = self.my_settings.output_genres_flag
        self.output_title_spoken_languages_flag = self.my_settings.output_title_spoken_languages_flag
        self.output_spoken_languages_flag = self.my_settings.output_spoken_languages_flag
        self.output_title_production_countries_flag = self.my_settings.output_title_production_countries_flag
        self.output_production_countries_flag = self.my_settings.output_production_countries_flag
        self.output_title_production_companies_flag = self.my_settings.output_title_production_companies_flag
        self.output_production_companies_flag = self.my_settings.output_production_companies_flag
        self.output_title_collections_flag = self.my_settings.output_title_collections_flag
        self.output_collections_flag = self.my_settings.output_collections_flag
        self.output_title_keywords_flag = self.my_settings.output_title_keywords_flag
        self.output_keywords_flag = self.my_settings.output_keywords_flag
        self.output_title_removed_flag = self.my_settings.output_title_removed_flag

    def check_title_exists(self, error_tmdb_id_list):
        
        # global api_key
        removed_titles = []

        # remove duplicates
        error_tmdb_id_list = list(set(error_tmdb_id_list))
        # list_len = len(error_tmdb_id_list)

        params = {
            'api_key': self.api_key,
            'language': 'en-US'
        }

        for i, tmdb_id in enumerate(error_tmdb_id_list):
            url = f'https://api.themoviedb.org/3/movie/{tmdb_id}'

            response = requests.get(url, params=params)
            results = response.json()
            if 'success' in results:
                removed_titles.append(tmdb_id)
            
            if (i + 1) % 40 == 0:
                # print(f'Titles processed: {i + 1} of {list_len}')
                time.sleep(2)
        
        if removed_titles:
            df = pd.DataFrame(removed_titles)
            df.columns = ['tmdb_id']
        else:
            df = pd.DataFrame()
        
        return df

    def get_full_movie_list(self):
        todays_date = datetime.datetime.now().strftime("%m_%d_%Y")
        url = f'http://files.tmdb.org/p/exports/movie_ids_{todays_date}.json.gz'
        data = []
        
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with gzip.open(response.raw, "rt", encoding="utf-8") as file:
                file_content = file.read()
                json_objects = file_content.strip().split('\n')
                for json_object in json_objects:
                    obj = json.loads(json_object)
                    data.append(obj)

                df_titles = pd.DataFrame(data)
                df_titles['as_of_date'] = datetime.date.today()
                df_titles = cleanse_title_df(df_titles)
                
                # print(data)
        else:
            df_titles = pd.DataFrame()

        return df_titles

    def get_movie_discover_data(self, movie_date=None, ids_to_skip=[], original_language=None, adult_content_flag='exclude', include_video=False, row_limit=1_000_000_000):
        
        if movie_date is not None:
            movie_date = dateparser.parse(movie_date).date().strftime('%Y-%m-%d')
        else:
            movie_date = dateparser.parse('yesterday').date().strftime('%Y-%m-%d')
        
        if adult_content_flag in ('include', 'only'):
            include_adult = True
        else:
            include_adult = False
        
        # global api_key
        url = 'https://api.themoviedb.org/3/discover/movie'
        params = {
            'api_key': self.api_key,
            'language': 'en-US',
            'include_adult': include_adult,
            'include_video': include_video,
            'page': 1
        }

        if movie_date:
            params['primary_release_date.gte'] = movie_date
            params['primary_release_date.lte'] = movie_date

        # if not original_language:
        #     original_language = 'en'
        
        if original_language:
            params['with_original_language'] = original_language
        
        all_results = []

        request_count = 0
        result_count = 0

        print('Retrieving discovered titles')

        while result_count < row_limit:
            response = requests.get(url, params=params)
            results = response.json()
            total_pages = int(results['total_pages'])
            request_count += 1
            if (request_count) % 40 == 0:
                print(f'Pages processed: {request_count} of {total_pages}')
                time.sleep(2)
            if 'total_pages' in results and results['total_pages'] > 0:
                all_results.extend(results['results'])
                result_count += len(results)
                
                if results['page'] < results['total_pages']:
                    params['page'] += 1
                else:
                    break
            else:
                break
        
        if adult_content_flag == 'exclude':
            all_results = [entry for entry in all_results if not entry.get('adult')]
        if adult_content_flag == 'only':
            all_results = [entry for entry in all_results if entry.get('adult')]
        
        # remove ids if skipped
        all_results = [d for d in all_results if d.get("id") not in ids_to_skip]
        
        if all_results:
            df = pd.DataFrame(all_results)
            df = cleanse_title_df(df)
        else:
            df = pd.DataFrame()
        
        return df

    def get_movie_ids_by_cast(self, person_ids, ids_to_skip=[]):
        """Retrieve film data given a list of Person IDs"""
        # global api_key
        tmdb_id_list = []

        # remove duplicates
        person_ids = list(set(person_ids))
        list_len = len(person_ids)

        params = {
            'api_key': self.api_key,
            'language': 'en-US'
        }

        print('Retrieving titles by cast')

        for i, person_id in enumerate(person_ids):
            url = f'https://api.themoviedb.org/3/person/{person_id}/movie_credits'

            response = requests.get(url, params=params)
            results = response.json()
            if 'success' not in results:
                tmdb_id_list.extend([entry['id'] for entry in results['cast']])
            else:
                self.local_db.error_person_id_list.append(person_id)
            
            if (i + 1) % 40 == 0:
                print(f'People processed: {i + 1} of {list_len}')
                time.sleep(2)
        
        tmdb_id_list = [item for item in tmdb_id_list if item not in ids_to_skip]
        
        if tmdb_id_list:
            return list(set(tmdb_id_list))
        
        return []

    def get_movie_ids_by_search_term(self, search_terms=[], ids_to_skip=[], original_language=None, adult_content_flag='exclude', row_limit=None):
        """Retrieve film data given a list of search terms"""

        if adult_content_flag in ('include', 'only'):
            include_adult = True
        else:
            include_adult = False
        
        if row_limit:
            search_terms = search_terms[:row_limit]
        else:
            row_limit = 1_000_000_000
        
        list_len = len(search_terms)
        
        # global api_key
        url = f'https://api.themoviedb.org/3/search/movie'
        params = {
            'api_key': self.api_key,
            'include_adult': include_adult,
            'page': 1
        }

        # if language:
        #     params['language'] = language
        
        all_results = []

        request_count = 0
        result_count = 0

        print('Retrieving titles by keywords')
        
        for count, search_term in enumerate(search_terms):
            if result_count >= row_limit:
                break
            params['query'] = search_term
            while result_count < row_limit:
                response = requests.get(url, params=params)
                results = response.json()
                request_count += 1
                if (request_count) % 40 == 0:
                    print(f'Search terms processed: {count} of {list_len}')
                    time.sleep(2)
                if 'total_pages' in results and results['total_pages'] > 0:
                    all_results.extend(results['results'])
                    result_count += len(results)
                    
                    if results['page'] < results['total_pages']:
                        params['page'] += 1
                    else:
                        break
                else:
                    break
        
        if adult_content_flag == 'exclude':
            all_results = [entry for entry in all_results if not entry.get('adult')]
        if adult_content_flag == 'only':
            all_results = [entry for entry in all_results if entry.get('adult')]
        
        # remove ids if skipped
        all_results = [d for d in all_results if d.get("id") not in ids_to_skip]

        # remove films not matching requested original language
        if original_language:
            all_results = [d for d in all_results if d.get("original_language") in original_language]
        
        if all_results:
            df = pd.DataFrame(all_results)
            df = df.drop_duplicates(subset=['id'], keep='first')

            return df['id'].to_list()
        
        return []

    def get_trending_movie_ids(self, time_window, ids_to_skip=[], original_language=None, row_limit=20_000):
        """Retrieve trending films"""

        # global api_key
        url = f'https://api.themoviedb.org/3/trending/movie/{time_window}'
        params = {
            'api_key': self.api_key,
            'page': 1
        }

        # if language:
        #     params['language'] = language
        
        all_results = []

        request_count = 0
        result_count = 0

        print('Retrieving trending titles')
        
        while result_count < row_limit:
            response = requests.get(url, params=params)
            results = response.json()
            list_len = results['total_results']
            if list_len > row_limit:
                list_len = row_limit
            request_count += 1
            result_count += 20
            if (request_count) % 40 == 0:
                print(f'Titles processed: {result_count} of {list_len}')
                time.sleep(2)
            if 'total_pages' in results and results['total_pages'] > 0:
                all_results.extend(results['results'])
                
                if results['page'] < results['total_pages']:
                    params['page'] += 1
                else:
                    break
            else:
                break
        
        # remove ids if skipped
        all_results = [d for d in all_results if d.get("id") not in ids_to_skip]

        # remove films not matching requested original language
        if all_results and original_language:
            all_results = [d for d in all_results if d.get("original_language") in original_language]
        
        if all_results:
            df = pd.DataFrame(all_results)
            df = df.drop_duplicates(subset=['id'], keep='first')

            return df['id'].to_list()
        
        return []

    def get_movie_changes(self, movie_date=None, adult_content_flag='exclude'):
        """Determine movies that have changed on a certain date"""
        # global api_key
        tmdb_id_list = []

        if movie_date is not None:
            movie_date = dateparser.parse(movie_date).date()
        else:
            movie_date = dateparser.parse('yesterday').date()
        
        url = 'https://api.themoviedb.org/3/movie/changes'

        params = {
            'api_key': self.api_key,
            'language': 'en-US',
            'start_date': movie_date,
            'end_date': movie_date,
            'page': 1
        }

        print('Retrieving changed titles')
        
        while True:
            response = requests.get(url, params=params)
            results = response.json()
            if 'total_pages' in results and results['total_pages'] > 0:
                tmdb_id_list.extend(results['results'])
                # if adult_content_flag:
                #     tmdb_id_list.extend(results['results'])
                # else:
                #     tmdb_id_list.extend([result for result in results['results'] if not result.get('adult')])
                
                if results['page'] < results['total_pages']:
                    params['page'] += 1
                else:
                    break
            else:
                break
        
        if adult_content_flag == 'exclude':
            tmdb_id_list = [entry for entry in tmdb_id_list if not entry.get('adult')]
        if adult_content_flag == 'only':
            tmdb_id_list = [entry for entry in tmdb_id_list if entry.get('adult')]
        
        if tmdb_id_list:
            tmdb_id_list = [item['id'] for item in tmdb_id_list]
            # df = pd.DataFrame(tmdb_id_list)

            # return cleanse_title_df(df)
            return tmdb_id_list
        
        return []

    def get_title_data(self, tmdb_id_list, original_language=None, min_runtime=None, row_limit=None):
        """Retrieve film data given a list of TMDB IDs"""
        # global api_key
        all_results = []

        # remove duplicates
        tmdb_id_list = list(set(tmdb_id_list))
        list_len = len(tmdb_id_list)
    
        if row_limit and tmdb_id_list and len(tmdb_id_list) >= row_limit:
            tmdb_id_list = tmdb_id_list[:row_limit]
            list_len = len(tmdb_id_list)

        params = {
            'api_key': self.api_key,
            'language': 'en-US'
        }

        print('Retrieving title data')

        for i, tmdb_id in enumerate(tmdb_id_list):
            url = f'https://api.themoviedb.org/3/movie/{tmdb_id}'

            response = requests.get(url, params=params)
            results = response.json()
            if 'success' not in results:
                if not original_language or (original_language and results['original_language'] == original_language):
                    if not min_runtime or (min_runtime and (int(results['runtime']) == 0 or int(results['runtime']) >= min_runtime)):
                        all_results.append(results)
            else:
                self.local_db.error_tmdb_id_list.append(tmdb_id)
            
            if (i + 1) % 40 == 0:
                print(f'Titles processed: {i + 1} of {list_len}')
                time.sleep(2)
        
        if all_results:
            df = pd.DataFrame(all_results)
            df = cleanse_title_df(df)
        else:
            df = pd.DataFrame()
        
        return df

    def process_title_data(self, df_titles, suffix):
        """Accept a dataframe of title data and explode and write out nested datasets"""
        def extract_genres(df_titles):

            df_genres = df_titles[df_titles['genres'].apply(lambda x: len(x) > 0)].copy()

            # 'Explode' the DataFrame copy on the 'genres' column
            df_genres = df_genres.explode('genres')

            # Create new columns 'genre_id' and 'genre_name' by mapping the 'id' and 'name' keys in the 'genres' dictionaries
            df_genres['genre_id'] = df_genres['genres'].map(lambda x: x['id'])
            df_genres['genre_name'] = df_genres['genres'].map(lambda x: x['name'])

            df_genres = df_genres.drop(columns=df_genres.columns.difference(['tmdb_id', 'genre_id', 'genre_name']))

            df_title_genre = df_genres[df_genres['genre_id'].notnull()][['tmdb_id', 'genre_id']].drop_duplicates().reset_index(drop=True)

            # Create df_genre
            df_genre = df_genres[df_genres['genre_id'].notnull()][['genre_id', 'genre_name']].drop_duplicates().reset_index(drop=True)

            df_titles.drop(columns=['genres'], inplace=True)

            return df_title_genre, df_genre

        def extract_spoken_languages(df_titles):

            # df_spoken_languages = df.copy().reset_index(drop=True)
            df_spoken_languages = df_titles[df_titles['spoken_languages'].apply(lambda x: len(x) > 0)].copy().reset_index(drop=True)

            # 'Explode' the DataFrame copy on the 'spoken_languages' column
            # df_spoken_languages = df_spoken_languages.explode('spoken_languages')
            df_spoken_languages = df_spoken_languages.explode('spoken_languages')

            # Create new columns 'spoken_language_id' and 'spoken_language_name' by mapping the 'id' and 'name' keys in the 'spoken_languages' dictionaries
            df_spoken_languages['spoken_language_english_name'] = df_spoken_languages['spoken_languages'].map(lambda x: x['english_name'])
            df_spoken_languages['spoken_language_iso_cd'] = df_spoken_languages['spoken_languages'].map(lambda x: x['iso_639_1'])
            df_spoken_languages['spoken_language_name'] = df_spoken_languages['spoken_languages'].map(lambda x: x['name'])

            df_spoken_languages = df_spoken_languages.drop(columns=df_spoken_languages.columns.difference(['tmdb_id', 'spoken_language_english_name', 'spoken_language_iso_cd', 'spoken_language_name']))

            df_title_spoken_language = df_spoken_languages[df_spoken_languages['spoken_language_iso_cd'].notnull()][['tmdb_id', 'spoken_language_iso_cd']].drop_duplicates().reset_index(drop=True)

            # Create df_spoken_language
            df_spoken_language = df_spoken_languages[df_spoken_languages['spoken_language_iso_cd'].notnull()][['spoken_language_iso_cd', 'spoken_language_name', 'spoken_language_english_name']].drop_duplicates().reset_index(drop=True)

            df_titles.drop(columns=['spoken_languages'], inplace=True)

            return df_title_spoken_language, df_spoken_language

        def extract_production_countries(df_titles):

            # df_production_countries = df.copy().reset_index(drop=True)
            df_production_countries = df_titles[df_titles['production_countries'].apply(lambda x: len(x) > 0)].copy().reset_index(drop=True)

            # 'Explode' the DataFrame copy on the 'production_countries' column
            # df_production_countries = df_production_countries.explode('production_countries')
            df_production_countries = df_production_countries.explode('production_countries')

            # Create new columns 'production_country_id' and 'production_country_name' by mapping the 'id' and 'name' keys in the 'production_countries' dictionaries
            df_production_countries['production_country_iso_cd'] = df_production_countries['production_countries'].map(lambda x: x['iso_3166_1'])
            df_production_countries['production_country_name'] = df_production_countries['production_countries'].map(lambda x: x['name'])

            df_production_countries = df_production_countries.drop(columns=df_production_countries.columns.difference(['tmdb_id', 'production_country_iso_cd', 'production_country_name']))

            df_title_production_country = df_production_countries[df_production_countries['production_country_iso_cd'].notnull()][['tmdb_id', 'production_country_iso_cd']].drop_duplicates().reset_index(drop=True)

            # Create df_production_country
            df_production_country = df_production_countries[df_production_countries['production_country_iso_cd'].notnull()][['production_country_iso_cd', 'production_country_name']].drop_duplicates().reset_index(drop=True)

            df_titles.drop(columns=['production_countries'], inplace=True)

            return df_title_production_country, df_production_country

        def extract_production_companies(df_titles):

            # df_production_companies = df.copy().reset_index(drop=True)
            df_production_companies = df_titles[df_titles['production_companies'].apply(lambda x: len(x) > 0)].copy().reset_index(drop=True)

            # 'Explode' the DataFrame copy on the 'production_companies' column
            # df_production_companies = df_production_companies.explode('production_companies')
            df_production_companies = df_production_companies.explode('production_companies')

            df_production_companies['production_company_id'] = df_production_companies['production_companies'].map(lambda x: x['id'])
            df_production_companies['production_company_name'] = df_production_companies['production_companies'].map(lambda x: x['name'])
            df_production_companies['production_company_logo_path'] = df_production_companies['production_companies'].map(lambda x: x['logo_path'])
            df_production_companies['production_company_origin_country'] = df_production_companies['production_companies'].map(lambda x: x['origin_country'])

            df_production_companies = df_production_companies.drop(columns=df_production_companies.columns.difference(['tmdb_id', 'production_company_id', 'production_company_name', 'production_company_logo_path', 'production_company_origin_country']))

            df_title_production_company = df_production_companies[df_production_companies['production_company_id'].notnull()][['tmdb_id', 'production_company_id']].drop_duplicates().reset_index(drop=True)

            # Create df_production_company
            df_production_company = df_production_companies[df_production_companies['production_company_id'].notnull()][['production_company_id', 'production_company_name', 'production_company_logo_path', 'production_company_origin_country']].drop_duplicates().reset_index(drop=True)

            df_titles.drop(columns=['production_companies'], inplace=True)

            return df_title_production_company, df_production_company

        def extract_collections(df_titles):

            # df_collections = df.copy().reset_index(drop=True)
            # df_collections = df[df['belongs_to_collection'].apply(lambda x: len(x) > 0)].copy().reset_index(drop=True)
            df_collections = df_titles[df_titles['belongs_to_collection'].apply(lambda x: x != None and len(x) > 0)].copy().reset_index(drop=True)

            # df_collections = df_collections.explode('belongs_to_collection')
            # df_collections = df_collections.explode('belongs_to_collection') # no explosion needed because this field is a single dictionary, instead of a list of dictionaries

            df_collections['collection_id'] = df_collections['belongs_to_collection'].map(lambda x: x['id'])
            df_collections['collection_name'] = df_collections['belongs_to_collection'].map(lambda x: x['name'])
            df_collections['collection_poster_path'] = df_collections['belongs_to_collection'].map(lambda x: x['poster_path'])
            df_collections['collection_backdrop_path'] = df_collections['belongs_to_collection'].map(lambda x: x['backdrop_path'])

            df_collections = df_collections.drop(columns=df_collections.columns.difference(['tmdb_id', 'collection_id', 'collection_name', 'collection_poster_path', 'collection_backdrop_path']))

            df_title_collection = df_collections[df_collections['collection_id'].notnull()][['tmdb_id', 'collection_id']].drop_duplicates().reset_index(drop=True)

            # Create df_collection
            df_collection = df_collections[df_collections['collection_id'].notnull()][['collection_id', 'collection_name', 'collection_poster_path', 'collection_backdrop_path']].drop_duplicates().reset_index(drop=True)

            df_titles.drop(columns=['belongs_to_collection'], inplace=True)

            return df_title_collection, df_collection

        api_result = {'action':'process_titles'}
        api_sub_results = []
        
        df_title_genre, df_genre = extract_genres(df_titles)

        df_title_spoken_language, df_spoken_language = extract_spoken_languages(df_titles)

        df_title_production_country, df_production_country = extract_production_countries(df_titles)

        df_title_production_company, df_production_company = extract_production_companies(df_titles)

        df_title_collection, df_collection = extract_collections(df_titles)

        output_path = self.output_path

        if self.output_titles_flag:
            filename = misc.write_data_to_file(df_titles, output_path + os.sep + 'tmdb_title', 'tmdb_title', suffix)
            # Update loaded titles list with tmdb_ids being extracted
            self.local_db.loaded_titles = df_titles['tmdb_id'].tolist()
            self.local_db.loaded_titles_adult = df_titles[df_titles['adult'] == True]['tmdb_id'].tolist()
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_titles):,}"}
            api_sub_results.append(api_sub_result)
        if self.output_title_genres_flag:
            filename = misc.write_data_to_file(df_title_genre, output_path + os.sep + 'tmdb_title_genre', 'tmdb_title_genre', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_title_genre):,}"}
        if self.output_genres_flag:
            filename = misc.write_data_to_file(df_genre, output_path + os.sep + 'tmdb_genre', 'tmdb_genre', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_genre):,}"}
        if self.output_title_spoken_languages_flag:
            filename = misc.write_data_to_file(df_title_spoken_language, output_path + os.sep + 'tmdb_title_spoken_language', 'tmdb_title_spoken_language', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_title_spoken_language):,}"}
        if self.output_spoken_languages_flag:
            filename = misc.write_data_to_file(df_spoken_language, output_path + os.sep + 'tmdb_spoken_language', 'tmdb_spoken_language', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_spoken_language):,}"}
        if self.output_title_production_countries_flag:
            filename = misc.write_data_to_file(df_title_production_country, output_path + os.sep + 'tmdb_title_production_country', 'tmdb_title_production_country', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_title_production_country):,}"}
        if self.output_production_countries_flag:
            filename = misc.write_data_to_file(df_production_country, output_path + os.sep + 'tmdb_production_country', 'tmdb_production_country', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_production_country):,}"}
        if self.output_title_production_companies_flag:
            filename = misc.write_data_to_file(df_title_production_company, output_path + os.sep + 'tmdb_title_production_company', 'tmdb_title_production_company', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_title_production_company):,}"}
        if self.output_production_companies_flag:
            filename = misc.write_data_to_file(df_production_company, output_path + os.sep + 'tmdb_production_company', 'tmdb_production_company', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_production_company):,}"}
        if self.output_title_collections_flag:
            filename = misc.write_data_to_file(df_title_collection, output_path + os.sep + 'tmdb_title_collection', 'tmdb_title_collection', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_title_collection):,}"}
        if self.output_collections_flag:
            filename = misc.write_data_to_file(df_collection, output_path + os.sep + 'tmdb_collection', 'tmdb_collection', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_collection):,}"}

        if api_sub_results:
            api_result['result'] = api_sub_results
            self.api_response.api_result.append(api_result)

    def process_title_data_subset(self, df_titles, suffix):
        """Accept a dataframe of data for all titles with limited fields"""
        
        api_result = {'action':'process_all_titles'}
        api_sub_results = []
        
        output_path = self.output_path

        filename = misc.write_data_to_file(df_titles, output_path + os.sep + 'tmdb_title_full', 'tmdb_title_full', suffix)
        api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_titles):,}"}
        api_sub_results.append(api_sub_result)

        if api_sub_results:
            api_result['result'] = api_sub_results
            self.api_response.api_result.append(api_result)
        
    def get_title_keyword_data(self, tmdb_id_list, row_limit=None):
        """Retrieve film keywords given a list of TMDB IDs"""
        # global api_key
        all_results = []

        # remove duplicates
        tmdb_id_list = list(set(tmdb_id_list))

        if row_limit and tmdb_id_list and len(tmdb_id_list) >= row_limit:
            tmdb_id_list = tmdb_id_list[:row_limit]
        
        list_len = len(tmdb_id_list)

        params = {
            'api_key': self.api_key,
            'language': 'en-US'
        }

        print('Retrieving title keywords')

        dummy_keywords = ({
                        'id': 0,
                        'keywords': [{'id': 0, 'name': 'no keywords'}]
                        })

        for i, tmdb_id in enumerate(tmdb_id_list):
            url = f'https://api.themoviedb.org/3/movie/{tmdb_id}/keywords'

            response = requests.get(url, params=params)
            results = response.json()
            if 'success' not in results:
                if not results['keywords']:
                    results['keywords'].append({'id': 0, 'name': 'no keywords'})
                all_results.append(results)
            else:
                dummy_keywords['id'] = tmdb_id
                all_results.append(dummy_keywords.copy())
                self.local_db.error_tmdb_id_list.append(tmdb_id)
            
            if (i + 1) % 40 == 0:
                print(f'Titles processed: {i + 1} of {list_len}')
                time.sleep(2)
        
        if all_results:
            df_title_keywords = pd.DataFrame(all_results)
            df_title_keywords = cleanse_title_df(df_title_keywords)
        else:
            df_title_keywords = pd.DataFrame()
        
        return df_title_keywords

    def process_title_keywords(self, df_title_keywords, suffix):
        """Accept a dataframe of title keyword data and explode and write out nested datasets"""
        def extract_keywords(df_title_keywords):

            df_keywords = df_title_keywords[df_title_keywords['keywords'].apply(lambda x: len(x) > 0)].copy()

            # 'Explode' the DataFrame copy on the 'keywords' column
            df_keywords = df_keywords.explode('keywords')

            # Create new columns 'keyword_id' and 'keyword_name' by mapping the 'id' and 'name' keys in the 'keywords' dictionaries
            df_keywords['keyword_id'] = df_keywords['keywords'].map(lambda x: x['id'])
            df_keywords['keyword_name'] = df_keywords['keywords'].map(lambda x: x['name'])

            df_keywords = df_keywords.drop(columns=df_keywords.columns.difference(['tmdb_id', 'keyword_id', 'keyword_name']))

            df_title_keyword = df_keywords[df_keywords['keyword_id'].notnull()][['tmdb_id', 'keyword_id']].drop_duplicates().reset_index(drop=True)

            df_keyword = df_keywords[df_keywords['keyword_id'].notnull()][['keyword_id', 'keyword_name']].drop_duplicates().reset_index(drop=True)

            # Fix this earlier
            df_keyword = df_keyword.applymap(lambda x: x.replace('\r\n', ' | ') if isinstance(x, str) else x)
            df_keyword = df_keyword.applymap(lambda x: x.replace('\r', ' | ') if isinstance(x, str) else x)
            df_keyword = df_keyword.applymap(lambda x: x.replace('\n', ' | ') if isinstance(x, str) else x)
            df_keyword = df_keyword.applymap(lambda x: x.replace('\t', ' ') if isinstance(x, str) else x)
            df_keyword = df_keyword.applymap(lambda x: x.replace('  ', ' ') if isinstance(x, str) else x)

            df_title_keywords.drop(columns=['keywords'], inplace=True)

            return df_title_keyword, df_keyword

        api_result = {'action':'process_title_keywords'}
        api_sub_results = []
        
        df_title_keyword, df_keyword = extract_keywords(df_title_keywords)

        output_path = self.output_path

        if self.output_title_keywords_flag:
            filename = misc.write_data_to_file(df_title_keyword, output_path + os.sep + 'tmdb_title_keyword', 'tmdb_title_keyword', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_title_keyword):,}"}
            api_sub_results.append(api_sub_result)
        if self.output_keywords_flag:
            filename = misc.write_data_to_file(df_keyword, output_path + os.sep + 'tmdb_keyword', 'tmdb_keyword', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_keyword):,}"}
            api_sub_results.append(api_sub_result)

        if api_sub_results:
            api_result['result'] = api_sub_results
            self.api_response.api_result.append(api_result)

    def process_removed_titles(self, df_removed_titles, suffix):

        api_result = {'action':'process_removed_titles'}
        api_sub_results = []
        
        output_path = self.output_path

        if self.output_title_removed_flag:
            filename = misc.write_data_to_file(df_removed_titles, output_path + os.sep + 'tmdb_title_removed', 'tmdb_title_removed', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_removed_titles):,}"}
            api_sub_results.append(api_sub_result)

        if api_sub_results:
            api_result['result'] = api_sub_results
            self.api_response.api_result.append(api_result)


# #%%
# if __name__ == '__main__':

#     import main, local_db_handler, person_handler, image_handler
#     from nexus_utils.database_utils import build_engine
#     from nexus_utils import password_utils as pw
#     from nexus_utils import datetime_utils as dtu

#     os.environ['PYDEVD_WARN_EVALUATION_TIMEOUT'] = '2000'
#     api_key = pw.get_password('keyring', password_key="APIKey", account_name="TMDB")
#     engine = main.engine
#     output_path = main.output_path
#     current_time, current_time_string, current_time_log = dtu.get_current_timestamp()

#     movie_data = MovieData(api_key, engine, output_path)
#     person_data = person_handler.PersonData(api_key, engine, output_path)

#     loaded_titles = local_db_handler.get_loaded_titles()

#     # df_tmdb_id_list = movie_data.get_movie_discover_data('01-01-2023', loaded_titles)

#     # df_tmdb_id_list = movie_data.determine_missing_keywords()

#     # movie_data.process_title_keywords(df_tmdb_id_list, current_time_string)

#     # movie_data.retrieve_movies_by_change_date()

#     # persons = [1535848, 1426252, 135660, 143070, 136331, 76575, 932319]
#     # persons = [1056772, 2953862, 997483, 591313]
#     persons = person_data.determine_missing_persons()
#     movies_list = movie_data.get_movie_ids_by_cast(persons, loaded_titles)
#     df_movies = movie_data.get_title_data(movies_list)
#     movie_data.process_title_data(df_movies, current_time_string)

#     # person_data.extract_missing_persons(current_time_string)

#     print('Done')
#     #%%
