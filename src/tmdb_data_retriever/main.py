#%%
import sys
import os
from pathlib import Path
import argparse
import dateparser
from nexus_utils.database_utils import build_engine
from nexus_utils import password_utils as pw
from nexus_utils import datetime_utils as dtu
import utils.connection_utils as conn
import utils.misc_utils as misc

#%%

def parse_command_run():
    """Interprets the arguments passed into the command line to run the correct function"""
    global local_db, movie_data, person_data, image_data, current_time, current_time_string, current_time_log
    import local_db_handler, movie_handler, person_handler, image_handler

    # current_time, current_time_string, current_time_log = dtu.get_current_timestamp()

    parser = argparse.ArgumentParser(description='Retrieve movie data')

    function_aliases = {
        'display_missing_counts': 'display_missing_counts',
        'dmc': 'display_missing_counts',
        'get_movies_updated_yesterday': 'get_movies_updated_yesterday',
        'gmuy': 'get_movies_updated_yesterday',
        'get_movies_by_favorite_actor': 'get_movies_by_favorite_actor',
        'gmbfa': 'get_movies_by_favorite_actor',
        'get_movies_by_search_terms': 'get_movies_by_search_terms',
        'gmbst': 'get_movies_by_search_terms',
        'get_trending_movies': 'get_trending_movies',
        'gtm': 'get_trending_movies',
        'get_missing_title_keywords': 'get_missing_title_keywords',
        'gmtk': 'get_missing_title_keywords',
        'get_missing_persons': 'get_missing_persons',
        'gmp': 'get_missing_persons',
        'get_missing_title_cast': 'get_missing_title_cast',
        'gmtc': 'get_missing_title_cast'
    }
    
    parser.add_argument('function', choices=function_aliases.keys(), help='Function to call')
    parser.add_argument('-lang', '--original_language', type=str, help='Original language')
    parser.add_argument('-rt', '--min_runtime', type=int, help='Minimum runtime in minutes')
    parser.add_argument('-adult', '--adult_content_flag', type=str, help='Adult content flag, accepts "include", "exclude" and "only"')
    parser.add_argument('-skip', '--skip_loaded_titles', action='store_true', help='Skip pulling titles already loaded')
    parser.add_argument('-search', '--search_terms', nargs='+', type=str, help='List of movie title keywords to search for')
    parser.add_argument('-pid', '--person_ids', nargs='+', type=int, help='List of person TMDB IDs')
    parser.add_argument('-rl', '--row_limit', type=int, help='Limit the number of rows returned')
    parser.add_argument('-tw', '--time_window', type=str, help='Time window for "Trending": Accepts "day" or "week"')
    
    # Determine if running from command line
    # run_from_command_line = sys.stdin.isatty()

    if run_from_command_line:
        args = parser.parse_args()
    else:
        # provide test values when developing / debugging
        args = argparse.Namespace(
            function='display_missing_counts'
        )
        # args = argparse.Namespace(
        #     function='get_movies_updated_yesterday',
        #     original_language='en',
        #     # min_runtime=30,
        #     adult_content_flag='o'
        # )

    # function = args.function
    function = function_aliases.get(args.function, args.function)
    original_language = getattr(args, 'original_language', None)
    min_runtime = getattr(args, 'min_runtime', None)
    adult_content_flag = getattr(args, 'adult_content_flag', None)
    if adult_content_flag:
        if adult_content_flag.lower() in ['include', 'i']:
            adult_content_flag = 'include'
        elif adult_content_flag.lower() in ['only', 'o']:
            adult_content_flag = 'only'
    else:
        if global_adult_content_flag:
            if global_adult_content_flag.lower() in ['include', 'i']:
                adult_content_flag = 'include'
            elif global_adult_content_flag.lower() in ['only', 'o']:
                adult_content_flag = 'only'
            else:
                adult_content_flag = 'exclude'
    skip_loaded_titles = getattr(args, 'skip_loaded_titles', None)
    search_terms = getattr(args, 'search_terms', None)
    person_id_list = getattr(args, 'person_ids', None)
    row_limit = getattr(args, 'row_limit', None)
    time_window = getattr(args, 'time_window', None)
    if time_window:
        if time_window.lower() in ['day', 'd']:
            time_window = 'day'
        elif time_window.lower() in ['week', 'w']:
            time_window = 'week'

    local_db = local_db_handler.LocalDB(
        engine,
        global_adult_content_flag,
        loaded_titles_sql=loaded_titles_sql,
        loaded_title_cast_sql=loaded_title_cast_sql,
        loaded_persons_sql=loaded_persons_sql,
        loaded_title_images_sql=loaded_title_images_sql,
        favorite_persons_sql=favorite_persons_sql,
        search_terms_sql=search_terms_sql,
        titles_missing_cast_sql=titles_missing_cast_sql,
        titles_missing_keywords_sql=titles_missing_keywords_sql,
        persons_missing_sql=persons_missing_sql
    )
    movie_data = movie_handler.MovieData(
        api_key, 
        local_db, 
        output_path, 
        output_titles_flag, 
        output_title_genres_flag, 
        output_genres_flag, 
        output_title_spoken_languages_flag, 
        output_spoken_languages_flag, 
        output_title_production_countries_flag, 
        output_production_countries_flag, 
        output_title_production_companies_flag, 
        output_production_companies_flag, 
        output_title_collections_flag, 
        output_collections_flag, 
        output_title_keywords_flag, 
        output_keywords_flag, 
        output_title_removed_flag
    )
    person_data = person_handler.PersonData(
        api_key, 
        local_db, 
        output_path, 
        output_persons_flag, 
        output_person_aka_flag, 
        output_title_cast_flag, 
        output_person_removed_flag)
    image_data = image_handler.ImageData(
            api_key, 
            local_db, 
            output_path, 
            images_path, 
            output_title_images_flag)

    if function == 'display_missing_counts':
        display_missing_counts(local_db)
        # if run_from_command_line:
        #     sys.exit()
        # else:
        return
    elif function == 'get_movies_updated_yesterday':
        print(f'Job Start: {current_time}')
        get_movies_updated_yesterday(original_language=original_language, min_runtime=min_runtime, adult_content_flag=adult_content_flag)
    elif function == 'get_movies_by_favorite_actor':
        print(f'Job Start: {current_time}')
        get_movies_by_favorite_actor(person_id_list=person_id_list, adult_content_flag=adult_content_flag, skip_loaded_titles=skip_loaded_titles, row_limit=row_limit)
    elif function == 'get_movies_by_search_terms':
        print(f'Job Start: {current_time}')
        get_movies_by_search_terms(search_terms=search_terms, original_language=original_language, adult_content_flag=adult_content_flag, skip_loaded_titles=skip_loaded_titles, row_limit=row_limit)
    elif function == 'get_trending_movies':
        print(f'Job Start: {current_time}')
        get_trending_movies(time_window=time_window, original_language=original_language, skip_loaded_titles=skip_loaded_titles, row_limit=row_limit)
    elif function == 'get_missing_title_keywords':
        print(f'Job Start: {current_time}')
        display_missing_counts(local_db)
        get_missing_title_keywords(adult_content_flag=adult_content_flag, row_limit=row_limit)
    elif function == 'get_missing_persons':
        print(f'Job Start: {current_time}')
        display_missing_counts(local_db)
        get_missing_persons(adult_content_flag=adult_content_flag, row_limit=row_limit)
    elif function == 'get_missing_title_cast':
        print(f'Job Start: {current_time}')
        display_missing_counts(local_db)
        get_missing_title_cast(adult_content_flag=adult_content_flag, row_limit=row_limit)
    else:
        valid_function_values = set(function_aliases.values())
        # valid_function_values = set(function_aliases.keys())
        valid_functions_string = ''.join([f'\n--{value}' for value in valid_function_values])
        print(f'Invalid function specified.  Valid values are:{valid_functions_string}')
        # if run_from_command_line:
        #     sys.exit()
        # else:
        return

    job_end_current_time, job_end_current_time_string, job_end_current_time_log = dtu.get_current_timestamp()
    days, hours, minutes, seconds, job_duration = dtu.get_duration(current_time, job_end_current_time)
    print(f'Job End: {job_end_current_time}\nTotal Duration: {job_duration}')

def display_missing_counts(local_db):
    # print(f'Global Adult Content Flag: {global_adult_content_flag}')
    print(f'Session Adult Content Flag: {local_db.adult_content_flag}')
    print(f'Missing Cast: {len(local_db.titles_missing_cast):,}')
    print(f'Missing Keywords:  {len(local_db.titles_missing_keywords):,}')
    print(f'Missing Persons:  {len(local_db.persons_missing):,}')

def handle_missing_data(suffix):
    
    if local_db.error_tmdb_id_list:
        df_removed_titles = movie_data.check_title_exists(local_db.error_tmdb_id_list)

        if len(df_removed_titles) > 0:
            movie_data.process_removed_titles(df_removed_titles, suffix)
    
    if local_db.error_person_id_list:
        df_removed_persons = person_data.check_person_exists(local_db.error_person_id_list)

        if len(df_removed_persons) > 0:
            person_data.process_removed_persons(df_removed_persons, suffix)

def get_movies_updated_yesterday(original_language=None, min_runtime=None, adult_content_flag=None):
    """Retrieve all movies marked as changed yesterday"""
    # TESTED
    yesterday = dateparser.parse('yesterday').date()
    formatted_date = yesterday.strftime('%Y-%m-%d')
    suffix = f'changed_{formatted_date}'

    if not adult_content_flag:
        if global_adult_content_flag:
            adult_content_flag = global_adult_content_flag
        else:
            adult_content_flag = 'exclude'
    
    changed_title_list = movie_data.get_movie_changes(formatted_date, adult_content_flag=adult_content_flag)

    df_titles = movie_data.get_title_data(changed_title_list, original_language=original_language, min_runtime=min_runtime)#, row_limit=50)

    if len(df_titles) > 0:
        movie_data.process_title_data(df_titles, suffix)
    else:
        print('No New Titles')

    handle_missing_data(suffix)

def get_movies_by_favorite_actor(person_id_list=[], adult_content_flag=None, skip_loaded_titles=True, row_limit=None):
    """Retrieve movies starring favorite actors.  If a list is not provided, will use local db values."""
    # TESTED
    suffix = current_time_string

    if not adult_content_flag:
        if global_adult_content_flag:
            adult_content_flag = global_adult_content_flag
        else:
            adult_content_flag = 'exclude'

    if not person_id_list:
        person_id_list = local_db.favorite_persons

    if skip_loaded_titles:
        ids_to_skip = local_db.loaded_titles
    else:
        ids_to_skip = []
    
    tmdb_id_list = person_data.get_titles_by_person(person_id_list, ids_to_skip=ids_to_skip, adult_content_flag=adult_content_flag, row_limit=row_limit)

    if tmdb_id_list:
        df_titles = movie_data.get_title_data(tmdb_id_list, original_language='en')

        if len(df_titles) > 0:
            movie_data.process_title_data(df_titles, suffix)
        else:
            print('No New Titles')
    else:
        print('No New Titles')

    handle_missing_data(suffix)

def get_movies_by_search_terms(search_terms=[], original_language=None, adult_content_flag=None, skip_loaded_titles=True, row_limit=None):
    """Retrieve movies by search terms.  If a list is not provided, will use local db values."""
    
    suffix = current_time_string

    if not adult_content_flag:
        if global_adult_content_flag:
            adult_content_flag = global_adult_content_flag
        else:
            adult_content_flag = 'exclude'

    if not original_language and global_original_language:
        original_language = global_original_language
    
    if not search_terms:
        search_terms = local_db.search_terms

    if skip_loaded_titles:
        ids_to_skip = local_db.loaded_titles
    else:
        ids_to_skip = []
    
    tmdb_id_list = movie_data.get_movie_ids_by_search_term(search_terms, ids_to_skip=ids_to_skip, original_language=original_language, adult_content_flag=adult_content_flag, row_limit=row_limit)

    if tmdb_id_list:
        df_titles = movie_data.get_title_data(tmdb_id_list, original_language='en')

        if len(df_titles) > 0:
            movie_data.process_title_data(df_titles, suffix)
        else:
            print('No New Titles')
    else:
        print('No New Titles')

    handle_missing_data(suffix)

def get_trending_movies(time_window=None, original_language=None, skip_loaded_titles=True, row_limit=None):
    """Retrieve movies by search terms.  If a list is not provided, will use local db values."""
    
    suffix = current_time_string

    if time_window.lower() == 'day':
        time_window = 'day'
    else:
        time_window = 'week'

    if not original_language and global_original_language:
        original_language = global_original_language
    
    if skip_loaded_titles:
        ids_to_skip = local_db.loaded_titles
    else:
        ids_to_skip = []
    
    tmdb_id_list = movie_data.get_trending_movie_ids(time_window, ids_to_skip=ids_to_skip, original_language=original_language, row_limit=row_limit)

    if tmdb_id_list:
        df_titles = movie_data.get_title_data(tmdb_id_list, original_language='en')
        
        if len(df_titles) > 0:
            movie_data.process_title_data(df_titles, suffix)
        else:
            print('No New Titles')
    else:
        print('No New Titles')

    handle_missing_data(suffix)

def get_title_images_by_persons(person_id_list, suffix=None, skip_loaded_titles=True, adult_content_flag=None, row_limit=None):
    # TESTED
    if not suffix:
        suffix = current_time_string

    if not adult_content_flag:
        if global_adult_content_flag:
            adult_content_flag = global_adult_content_flag
        else:
            adult_content_flag = 'exclude'
    
    tmdb_id_list = person_data.get_titles_by_person(person_id_list, adult_content_flag=adult_content_flag)
    
    # tmdb_id_list = [220030, 475176]

    if skip_loaded_titles:
        ids_to_skip = local_db.loaded_titles
        tmdb_id_list = [item for item in tmdb_id_list if item not in ids_to_skip]
    else:
        ids_to_skip = []
    
    if row_limit and tmdb_id_list and len(tmdb_id_list) >= row_limit:
        tmdb_id_list = tmdb_id_list[:row_limit]

    if tmdb_id_list:
        df_images = image_data.get_image_data(tmdb_id_list, ids_to_skip)

        if len(df_images) > 0:
            image_data.process_images(df_images, suffix)
        else:
            print('No images to process')
    else:
        print('No images to process')

    handle_missing_data(suffix)

def get_missing_title_keywords(adult_content_flag=None, row_limit=None):
    
    suffix = current_time_string

    if not adult_content_flag:
        if global_adult_content_flag:
            adult_content_flag = global_adult_content_flag
        else:
            adult_content_flag = 'exclude'
    
    tmdb_id_list = local_db.titles_missing_keywords

    if tmdb_id_list:
        df_title_keywords = movie_data.get_title_keyword_data(tmdb_id_list, row_limit=row_limit)

        if len(df_title_keywords) > 0:
            movie_data.process_title_keywords(df_title_keywords, suffix)
        else:
            print('No missing title keywords')
    else:
        print('No missing title keywords')

    handle_missing_data(suffix)

def get_missing_persons(adult_content_flag=None, row_limit=None):
    
    suffix = current_time_string

    if not adult_content_flag:
        if global_adult_content_flag:
            adult_content_flag = global_adult_content_flag
        else:
            adult_content_flag = 'exclude'

    person_id_list = local_db.persons_missing

    if person_id_list:
        df_person = person_data.get_person_data(person_id_list, row_limit=row_limit)

        if len(df_person) > 0:
            person_data.process_persons(df_person, suffix)
        else:
            print('No missing persons')
    else:
        print('No missing persons')

    handle_missing_data(suffix)

def get_missing_title_cast(adult_content_flag=None, row_limit=None):
    
    suffix = current_time_string

    if not adult_content_flag:
        if global_adult_content_flag:
            adult_content_flag = global_adult_content_flag
        else:
            adult_content_flag = 'exclude'
    
    tmdb_id_list = local_db.titles_missing_cast

    if tmdb_id_list:
        df_title_cast = person_data.get_title_cast_data_by_movie(tmdb_id_list, row_limit=row_limit)
        
        if len(df_title_cast) > 0:
            person_data.process_title_cast(df_title_cast, suffix)
        else:
            print('No missing title cast')
    else:
        print('No missing title cast')

    handle_missing_data(suffix)


#%%
os.environ['PYDEVD_WARN_EVALUATION_TIMEOUT'] = '2000'
run_from_command_line = sys.stdin.isatty()

current_dir = os.getcwd()
# project_dir = current_dir

config_path = Path(current_dir).parent.parent / 'config'
app_config_path = config_path / "app_config.ini"
connection_config_path = config_path / "connections_config.ini"
sql_queries_config_path = config_path / "sql_queries"
output_file_config_path = config_path / "output_files_config.ini"

(
    output_path, 
    images_path, 
    log_file_path, 
    password_method, 
    password_access_key, 
    password_secret_key, 
    password_endpoint_url, 
    password_region_name, 
    password_password_path, 
    read_chunk_size, 
    archive_flag, 
    logging_flag, 
    log_archive_expire_days, 
    global_original_language, 
    global_adult_content_flag
    ) = conn.read_app_config_settings(app_config_path)

if global_adult_content_flag and global_adult_content_flag not in ('include', 'exclude', 'only'):
    global_adult_content_flag = 'exclude'

db_target_config = 'target_connection'
(
    connect_type, 
    server_address, 
    server_port, 
    database_name, 
    schema, 
    user_name, 
    secret_key
    ) = conn.read_connection_config_settings(connection_config_path, db_target_config)
db_password = pw.get_password(password_method, secret_key, account_name=user_name, access_key=password_access_key, secret_key=password_secret_key, endpoint_url=password_endpoint_url, region_name=password_region_name, password_path=password_password_path)

(
    connect_type, 
    server_address, 
    server_port, 
    database_name, 
    schema, 
    user_name, 
    secret_key
    ) = conn.read_connection_config_settings(connection_config_path, db_target_config)
engine = build_engine(connect_type, server_address, server_port, database_name, user_name, db_password)#, schema)

api_source_config = 'tmdb_api_connection'
(
    api_password_method, 
    password_access_key, 
    password_secret_key, 
    password_endpoint_url, 
    password_region_name, 
    password_password_path, 
    api_user_name, 
    api_secret_key
    ) = conn.read_connection_config_settings(connection_config_path, api_source_config)
api_key = pw.get_password(api_password_method, password_key=api_secret_key, account_name=api_user_name)

(
    loaded_titles_sql, 
    loaded_title_cast_sql, 
    loaded_persons_sql, 
    loaded_title_images_sql, 
    favorite_persons_sql, 
    search_terms_sql, 
    titles_missing_cast_sql, 
    titles_missing_keywords_sql, 
    persons_missing_sql
    ) = misc.read_sql_queries(sql_queries_config_path)
# loaded_titles_sql, loaded_title_cast_sql, loaded_persons_sql, loaded_title_images_sql, favorite_persons_sql, search_terms_sql, titles_missing_cast_sql, titles_missing_keywords_sql, persons_missing_sql = conn.read_sql_queries(sql_queries_config_path)

(
    output_titles_flag, 
    output_title_genres_flag, 
    output_genres_flag, 
    output_title_spoken_languages_flag, 
    output_spoken_languages_flag, 
    output_title_production_countries_flag, 
    output_production_countries_flag, 
    output_title_production_companies_flag, 
    output_production_companies_flag, 
    output_title_collections_flag, 
    output_collections_flag, 
    output_title_keywords_flag, 
    output_keywords_flag, 
    output_persons_flag, 
    output_person_aka_flag, 
    output_title_cast_flag, 
    output_title_images_flag, 
    output_person_removed_flag, 
    output_title_removed_flag
    ) = conn.read_output_file_flags(output_file_config_path)

current_time, current_time_string, current_time_log = dtu.get_current_timestamp()

local_db = None
movie_data = None
person_data = None
image_data = None
#%%

if __name__ == '__main__':
    if run_from_command_line:
        parse_command_run()
    else:
        import local_db_handler, movie_handler, person_handler, image_handler
        
        # parse_command_run()
        
        # current_time, current_time_string, current_time_log = dtu.get_current_timestamp()

        print(f'Job Start: {current_time}')

        local_db = local_db_handler.LocalDB(
            engine,
            global_adult_content_flag,
            loaded_titles_sql=loaded_titles_sql,
            loaded_title_cast_sql=loaded_title_cast_sql,
            loaded_persons_sql=loaded_persons_sql,
            loaded_title_images_sql=loaded_title_images_sql,
            favorite_persons_sql=favorite_persons_sql,
            search_terms_sql=search_terms_sql,
            titles_missing_cast_sql=titles_missing_cast_sql,
            titles_missing_keywords_sql=titles_missing_keywords_sql,
            persons_missing_sql=persons_missing_sql
        )
        # print(len(local_db.loaded_titles))
        movie_data = movie_handler.MovieData(
            api_key, 
            local_db, 
            output_path, 
            output_titles_flag, 
            output_title_genres_flag, 
            output_genres_flag, 
            output_title_spoken_languages_flag, 
            output_spoken_languages_flag, 
            output_title_production_countries_flag, 
            output_production_countries_flag, 
            output_title_production_companies_flag, 
            output_production_companies_flag, 
            output_title_collections_flag, 
            output_collections_flag, 
            output_title_keywords_flag, 
            output_keywords_flag, 
            output_title_removed_flag
        )
        person_data = person_handler.PersonData(
            api_key, 
            local_db, 
            output_path, 
            output_persons_flag, 
            output_person_aka_flag, 
            output_title_cast_flag, 
            output_person_removed_flag)
        image_data = image_handler.ImageData(
            api_key, 
            local_db, 
            output_path, 
            images_path, 
            output_title_images_flag)

        display_missing_counts(local_db)

        # get_movies_by_search_terms(original_language='en', skip_loaded_titles=True)#, row_limit=12)
        # get_trending_movies(time_window='week', original_language='en', skip_loaded_titles=True, row_limit=2_000)

        # get_movies_by_favorite_actor([74252], adult_content_flag=True, ids_to_skip=loaded_titles)
        # get_movies_by_favorite_actor(ids_to_skip=loaded_titles)

        # get_movies_updated_yesterday(original_language='en', adult_content_flag='include')
        # get_movies_updated_yesterday(original_language='en')#, adult_content_flag='only')

        # movie_data.get_movie_discover_data(adult_content_flag='only')

        # get_title_images_by_persons([1912793], skip_loaded_titles=False)

        # get_missing_persons(current_time_string, [1535848, 1426252, 135660, 143070, 136331, 76575, 932319, 1056772, 2953862, 997483, 591313])
        # get_missing_title_cast()
        get_missing_persons()
        # get_missing_title_keywords()

        # tmdb_list = person_data.get_titles_by_person([1535848, 1426252, 135660, 143070, 136331, 76575, 932319, 1056772, 2953862, 997483, 591313], local_db.loaded_title_cast)

        # person_data.get_title_cast_data_by_movie(tmdb_list)

        # person_data.get_title_cast_data_by_movie([1064688, 364385])

        # get_missing_title_cast(row_limit=100)
        # get_missing_persons(row_limit=1_000)

        # get_missing_title_keywords(row_limit=200)
        # get_missing_persons()

        # get_movies_by_favorite_actor(adult_content_flag=True, ids_to_skip=local_db.loaded_titles)

        job_end_current_time, job_end_current_time_string, job_end_current_time_log = dtu.get_current_timestamp()
        days, hours, minutes, seconds, job_duration = dtu.get_duration(current_time, job_end_current_time)
        print(f'Job End: {job_end_current_time}\nTotal Duration: {job_duration}')

        #%%