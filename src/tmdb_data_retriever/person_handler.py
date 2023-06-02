#%%
import os
import requests
import pandas as pd
# from sqlalchemy import text
import time
import utils.misc_utils as misc


def cleanse_person_df(df):

    # df = df.applymap(lambda x: x.replace('\r\n', ' | ') if isinstance(x, str) else x)
    # df = df.applymap(lambda x: x.replace('\r', ' | ') if isinstance(x, str) else x)
    # df = df.applymap(lambda x: x.replace('\n', ' | ') if isinstance(x, str) else x)
    # df = df.applymap(lambda x: x.replace('\t', ' ') if isinstance(x, str) else x)
    # df = df.applymap(lambda x: x.replace('  ', ' ') if isinstance(x, str) else x)
    df = df.applymap(lambda x: misc.cleanse_value(x))
    df = df.rename(columns={'id': 'person_id', 'name': 'person_name'})

    return df

class PersonData:
    def __init__(
            self, 
            api_key, 
            local_db, 
            output_path, 
            output_persons_flag=True, 
            output_person_aka_flag=True, 
            output_title_cast_flag=True, 
            output_person_removed_flag=True):
        
        self.api_key = api_key
        self.local_db = local_db
        self.output_path = output_path
        self.output_persons_flag = output_persons_flag
        self.output_person_aka_flag = output_person_aka_flag
        self.output_title_cast_flag = output_title_cast_flag
        self.output_person_removed_flag = output_person_removed_flag

    def check_person_exists(self, error_person_id_list):
        
        # global api_key
        removed_persons = []

        # remove duplicates
        error_person_id_list = list(set(error_person_id_list))
        # list_len = len(error_person_id_list)

        params = {
            'api_key': self.api_key,
            'language': 'en-US'
        }

        for i, person_id in enumerate(error_person_id_list):
            url = f'https://api.themoviedb.org/3/person/{person_id}'

            response = requests.get(url, params=params)
            results = response.json()
            if 'success' in results:
                removed_persons.append(person_id)
            
            if (i + 1) % 40 == 0:
                # print(f'Persons processed: {i + 1} of {list_len}')
                time.sleep(2)
        
        if removed_persons:
            df = pd.DataFrame(removed_persons)
            df.columns = ['person_id']
        else:
            df = pd.DataFrame()
        
        return df

    def get_title_cast_data_by_movie(self, tmdb_id_list, row_limit=None):
        """Retrieve cast data given a list of TMDB IDs"""
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

        print('Retrieving cast by titles')

        dummy_title_cast = ({
                        'adult': False,
                        'gender': 0,
                        'id': 0,
                        'known_for_department': 'None',
                        'name': 'No Name',
                        'original_name': 'No Name',
                        'popularity': 0,
                        'profile_path': None,
                        'cast_id': 0,
                        'character': None,
                        'credit_id': '0',
                        'order': 0
                        })

        for i, tmdb_id in enumerate(tmdb_id_list):
            url = f'https://api.themoviedb.org/3/movie/{tmdb_id}/credits'

            response = requests.get(url, params=params)
            results = response.json()
            if 'success' not in results:
                if not results['cast']:
                    dummy_title_cast['tmdb_id'] = tmdb_id
                    dummy_title_cast['credit_id'] = str(int(tmdb_id) * -1)
                    results['cast'].append(dummy_title_cast.copy())
                else:
                    for cast_item in results['cast']:
                        cast_item['tmdb_id'] = results['id']
                all_results.extend(results['cast'])
            else:
                dummy_title_cast['tmdb_id'] = tmdb_id
                dummy_title_cast['credit_id'] = str(int(tmdb_id) * -1)
                all_results.append(dummy_title_cast.copy())
                self.local_db.error_tmdb_id_list.append(tmdb_id)
            
            if (i + 1) % 40 == 0:
                print(f'Titles processed: {i + 1} of {list_len}')
                time.sleep(2)
        
        if all_results:
            df_title_cast = pd.DataFrame(all_results)
            df_title_cast = cleanse_person_df(df_title_cast)
        else:
            df_title_cast = pd.DataFrame()
        
        return df_title_cast

    def get_titles_by_person(self, person_id_list, ids_to_skip=[], adult_content_flag='exclude', row_limit=None):
        """Retrieve cast data given a list of TMDB IDs"""
        # global api_key
        tmdb_id_list = []

        # remove duplicates
        person_id_list = list(set(person_id_list))

        if row_limit:
            person_id_list = person_id_list[:row_limit]
        
        list_len = len(person_id_list)

        params = {
            'api_key': self.api_key,
            'language': 'en-US'
        }

        print('Retrieving titles by person')

        for i, person_id in enumerate(person_id_list):
            url = f'https://api.themoviedb.org/3/person/{person_id}/movie_credits'

            response = requests.get(url, params=params)
            results = response.json()
            if 'success' not in results:
                if adult_content_flag == 'include':
                    tmdb_id_list.extend([entry['id'] for entry in results['cast']])
                elif adult_content_flag == 'exclude':
                    tmdb_id_list.extend([entry['id'] for entry in results['cast'] if not entry['adult']])
                elif adult_content_flag == 'only':
                    tmdb_id_list.extend([entry['id'] for entry in results['cast'] if entry['adult']])
            else:
                self.local_db.error_person_id_list.append(person_id)
            
            if (i + 1) % 40 == 0:
                print(f'Persons processed: {i + 1} of {list_len}')
                time.sleep(2)
        
        tmdb_id_list = [item for item in tmdb_id_list if item not in ids_to_skip]
        
        if tmdb_id_list:
            return list(set(tmdb_id_list))
        
        return []

    def get_person_data(self, person_id_list=[], ids_to_skip=[], row_limit=None):
        """Retrieve person data given a list of TMDB Person IDs"""
        # global api_key
        all_results = []

        # remove duplicates
        person_id_list = list(set(person_id_list))

        if ids_to_skip:
            person_id_list = [item for item in person_id_list if item not in ids_to_skip]
    
        if row_limit and person_id_list and len(person_id_list) >= row_limit:
            person_id_list = person_id_list[:row_limit]
        
        list_len = len(person_id_list)

        params = {
            'api_key': self.api_key,
            'language': 'en-US'
        }

        print('Retrieving person data')

        for i, person_id in enumerate(person_id_list):
            url = f'https://api.themoviedb.org/3/person/{person_id}'

            response = requests.get(url, params=params)
            results = response.json()
            if 'success' not in results:
                all_results.append(results)
            else:
                self.local_db.error_person_id_list.append(person_id)
            
            if (i + 1) % 40 == 0:
                print(f'Persons processed: {i + 1} of {list_len}')
                time.sleep(2)
        
        if all_results:
            df = pd.DataFrame(all_results)
            df = cleanse_person_df(df)
        else:
            df = pd.DataFrame()
        
        return df

    def process_persons(self, df_person, suffix):
        """Accept a dataframe of person data and explode and write out nested datasets"""
        def extract_person_aka(df):

            df_person_aka = df[df['also_known_as'].apply(lambda x: x != None and len(x) > 0)].copy().reset_index(drop=True)

            df_person_aka = df_person_aka.explode('also_known_as')
            df_person_aka = df_person_aka.rename(columns={'also_known_as': 'aka_name'})

            df_person_aka = df_person_aka.drop(columns=df_person_aka.columns.difference(['person_id', 'aka_name']))

            df_person_aka = df_person_aka[df_person_aka['aka_name'].notnull()][['person_id', 'aka_name']].drop_duplicates().reset_index(drop=True)

            # Fix this earlier
            df_person_aka = df_person_aka.applymap(lambda x: x.replace('\r\n', ' | ') if isinstance(x, str) else x)
            df_person_aka = df_person_aka.applymap(lambda x: x.replace('\r', ' | ') if isinstance(x, str) else x)
            df_person_aka = df_person_aka.applymap(lambda x: x.replace('\n', ' | ') if isinstance(x, str) else x)
            df_person_aka = df_person_aka.applymap(lambda x: x.replace('\t', ' ') if isinstance(x, str) else x)
            df_person_aka = df_person_aka.applymap(lambda x: x.replace('  ', ' ') if isinstance(x, str) else x)

            df.drop(columns=['also_known_as'], inplace=True)

            return df_person_aka

        df_person_aka = extract_person_aka(df_person)

        output_path = self.output_path

        if self.output_persons_flag:
            misc.write_data_to_file(df_person, output_path + os.sep + 'tmdb_person', 'tmdb_person', suffix)
        if self.output_person_aka_flag:
            misc.write_data_to_file(df_person_aka, output_path + os.sep + 'tmdb_person_aka', 'tmdb_person_aka', suffix)

    # def get_title_cast(self, suffix, tmdb_id_list=[], row_limit=None):

    #     if not tmdb_id_list:
    #         tmdb_id_list = self.local_db.titles_missing_cast

    #     if row_limit and tmdb_id_list and len(tmdb_id_list) >= row_limit:
    #         tmdb_id_list = tmdb_id_list[:row_limit]
        
    #     df_title_cast = self.get_title_cast_data_by_movie(tmdb_id_list)

    #     if len(df_title_cast) > 0:
    #         self.process_title_cast(df_title_cast, suffix)
    #     else:
    #         print('No title cast available')

    def process_title_cast(self, df_title_cast, suffix):

        output_path = self.output_path

        if self.output_title_cast_flag:
            misc.write_data_to_file(df_title_cast, output_path + os.sep + 'tmdb_title_cast', 'tmdb_title_cast', suffix)

    def process_removed_persons(self, df_removed_persons, suffix):

        output_path = self.output_path

        if self.output_person_removed_flag:
            misc.write_data_to_file(df_removed_persons, output_path + os.sep + 'tmdb_person_removed', 'tmdb_person_removed', suffix)

# #%%
# if __name__ == '__main__':

#     import main, local_db_handler, movie_handler, image_handler
#     from nexus_utils import password_utils as pw
#     from nexus_utils import datetime_utils as dtu
#     os.environ['PYDEVD_WARN_EVALUATION_TIMEOUT'] = '2000'
#     api_key = pw.get_password('keyring', password_key="APIKey", account_name="TMDB")
#     engine = main.engine
#     output_path = main.output_path
#     current_time, current_time_string, current_time_log = dtu.get_current_timestamp()

#     local_db = local_db_handler.LocalDB(engine)
#     movie_data = movie_handler.MovieData(api_key, engine, output_path)
#     person_data = PersonData(api_key, engine, output_path)

#     loaded_titles = local_db.get_loaded_titles()

#     # person_data.get_title_cast(current_time_string)

#     # person_data.extract_missing_persons(current_time_string)

#     person_data.extract_missing_title_cast(current_time_string)
#     #%%
