#%%
import os
import requests
import json
import gzip
import pandas as pd
# from sqlalchemy import text
import datetime
import time
# import utils.misc_utils as misc
from src.tmdb_data_retriever.utils import misc_utils as misc

#%%

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
            my_settings, 
            # api_key, 
            local_db, 
            api_response, 
            # output_path, 
            # output_persons_flag=True, 
            # output_person_aka_flag=True, 
            # output_title_cast_flag=True, 
            # output_person_removed_flag=True
            ):
        
        self.my_settings = my_settings
        self.api_key = self.my_settings.api_key
        self.local_db = local_db
        self.api_response = api_response
        self.output_path = self.my_settings.output_path
        self.output_persons_flag = self.my_settings.output_persons_flag
        self.output_person_aka_flag = self.my_settings.output_person_aka_flag
        self.output_title_cast_flag = self.my_settings.output_title_cast_flag
        self.output_person_removed_flag = self.my_settings.output_person_removed_flag

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

    def get_full_person_list(self):
        todays_date = datetime.datetime.now().strftime("%m_%d_%Y")
        url = f'http://files.tmdb.org/p/exports/person_ids_{todays_date}.json.gz'
        data = []
        
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with gzip.open(response.raw, "rt", encoding="utf-8") as file:
                file_content = file.read()
                json_objects = file_content.strip().split('\n')
                for json_object in json_objects:
                    obj = json.loads(json_object)
                    data.append(obj)

                df_persons = pd.DataFrame(data)
                df_persons['as_of_date'] = datetime.date.today()
                df_persons = cleanse_person_df(df_persons)
                
                # print(data)
        else:
            df_persons = pd.DataFrame()

        return df_persons

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
        
        retry_count = 0
        retry_limit = 3
        processed_at_last_error = 0

        try:
            for i, tmdb_id in enumerate(tmdb_id_list):
                while True:
                    url = f'https://api.themoviedb.org/3/movie/{tmdb_id}/credits'

                    try:
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
                        break
                    except Exception as e:
                        # If at least 50 records were processed since last error, reset the retry count
                        if i - processed_at_last_error >= 50:
                            processed_at_last_error = i
                            retry_count = -1
                        
                        retry_count += 1
                        if retry_count < retry_limit:
                            print('Error encountered, retrying in 5 minutes...')
                            time.sleep(300)
                        else:
                            raise Exception('Retries exceeded')
        except Exception as e:
            if all_results:
                message = f'"get_title_cast_data_by_movie" did not fully complete: {i} of {list_len} processed'
            else:
                message = f'Error encountered: {str(e)}'
            print(message)
            self.api_response.append_message(message)
        
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

        if len(person_id_list) > 1:
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

        if len(person_id_list) > 1:
            print('Retrieving person data')
            
        retry_count = 0
        retry_limit = 3
        processed_at_last_error = 0

        try:
            for i, person_id in enumerate(person_id_list):
                while True:
                    url = f'https://api.themoviedb.org/3/person/{person_id}'

                    try:
                        response = requests.get(url, params=params)
                        results = response.json()
                        if 'success' not in results:
                            all_results.append(results)
                        else:
                            self.local_db.error_person_id_list.append(person_id)
                        
                        if (i + 1) % 40 == 0:
                            print(f'Persons processed: {i + 1} of {list_len}')
                            time.sleep(2)
                        break
                    except Exception as e:
                        # If at least 50 records were processed since last error, reset the retry count
                        if i - processed_at_last_error >= 50:
                            processed_at_last_error = i
                            retry_count = -1
                        
                        retry_count += 1
                        if retry_count < retry_limit:
                            print('Error encountered, retrying in 5 minutes...')
                            time.sleep(300)
                        else:
                            raise Exception('Retries exceeded')
        except Exception as e:
            if all_results:
                message = f'"get_person_data" did not fully complete: {i} of {list_len} processed'
            else:
                message = f'Error encountered: {str(e)}'
            print(message)
            self.api_response.append_message(message)
        
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

        api_result = {'action':'process_persons'}
        api_sub_results = []
        
        df_person_aka = extract_person_aka(df_person)

        output_path = self.output_path

        if self.output_persons_flag:
            filename = misc.write_data_to_file(df_person, output_path + os.sep + 'tmdb_person', 'tmdb_person', suffix)
            # Update loaded persons list with person_ids being extracted
            self.local_db.loaded_persons = df_person['person_id'].tolist()
            self.local_db.loaded_persons_adult = df_person[df_person['adult'] == True]['person_id'].tolist()
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_person):,}"}
            api_sub_results.append(api_sub_result)
        if self.output_person_aka_flag:
            filename = misc.write_data_to_file(df_person_aka, output_path + os.sep + 'tmdb_person_aka', 'tmdb_person_aka', suffix)
            api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_person_aka):,}"}
            api_sub_results.append(api_sub_result)

        if api_sub_results:
            api_result['result'] = api_sub_results
            self.api_response.api_result.append(api_result)

    def process_person_data_subset(self, df_persons, suffix):
        """Accept a dataframe of data for all persons with limited fields"""
        
        api_result = {'action':'process_all_persons'}
        api_sub_results = []
        
        output_path = self.output_path

        filename = misc.write_data_to_file(df_persons, output_path + os.sep + 'tmdb_person_full', 'tmdb_person_full', suffix)
        api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_persons):,}"}
        api_sub_results.append(api_sub_result)

        if api_sub_results:
            api_result['result'] = api_sub_results
            self.api_response.api_result.append(api_result)
        
    def process_title_cast(self, df_title_cast, suffix):

        api_result = {'action':'process_title_cast'}
        api_sub_results = []
        
        output_path = self.output_path

        if self.output_title_cast_flag:
            filename = misc.write_data_to_file(df_title_cast, output_path + os.sep + 'tmdb_title_cast', 'tmdb_title_cast', suffix)
        api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_title_cast):,}"}
        api_sub_results.append(api_sub_result)

        if api_sub_results:
            api_result['result'] = api_sub_results
            self.api_response.api_result.append(api_result)

    def process_removed_persons(self, df_removed_persons, suffix):

        api_result = {'action':'process_removed_persons'}
        api_sub_results = []
        
        output_path = self.output_path

        if self.output_person_removed_flag:
            filename = misc.write_data_to_file(df_removed_persons, output_path + os.sep + 'tmdb_person_removed', 'tmdb_person_removed', suffix)
        api_sub_result = {'filename':f"{filename}", 'record_count':f"{len(df_removed_persons):,}"}
        api_sub_results.append(api_sub_result)

        if api_sub_results:
            api_result['result'] = api_sub_results
            self.api_response.api_result.append(api_result)

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

# %%
