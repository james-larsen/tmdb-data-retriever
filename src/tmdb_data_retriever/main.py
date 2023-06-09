#%%
import sys
import os
from pathlib import Path
import argparse
import dateparser
import datetime
from flask import Flask, request, make_response, jsonify
import json
from threading import Lock
import inspect
from nexus_utils.database_utils import build_engine
from nexus_utils import password_utils as pw
from nexus_utils import datetime_utils as dtu
from nexus_utils import string_utils
# import utils.connection_utils as conn
from src.tmdb_data_retriever.utils import connection_utils as conn
# import utils.misc_utils as misc
from src.tmdb_data_retriever.utils import misc_utils as misc
from src.tmdb_data_retriever import settings

#%%

# def test_request():
#     import requests

#     # Define the URL of the endpoint
#     url = 'http://localhost:5000/trigger_function'

#     # Define the payload
#     payload = {
#         'function_name': 'display_missing_counts'
#     }

#     # Send the POST request
#     response = requests.post(url, json=payload)

#     # Check the response status code
#     if response.status_code == 200:
#         print('Request successful')
#     else:
#         print(f'Request failed with status code: {response.status_code}')

app = Flask(__name__)
lock = Lock()

def run_flask_app(host=None, port=None):
    from waitress import serve

    if not host:
        host = 'localhost'

    if not port:
        port = 5002

    print(f'Listening on {host}:{port}...')
    serve(app, host=host, port=port)
    # app.run(host=host, port=port)

def build_api_response():
    global api_result, api_error_flag, api_error_message

    with app.app_context():
        if api_error_flag:
            if not api_result:
                api_result = {}
            response_data = {
                'status': 'error',
                'message': api_error_message,
                'result': {}
            }
            api_response = make_response(jsonify(response_data), 400)
        else:
            response_data = {
                'status': 'success',
                'message': 'Completed successfully',
                'result': api_result
            }
            api_response = make_response(jsonify(response_data), 200)

        api_response.headers['Content-Type'] = 'application/json'
        # Decode with: "json.loads(api_response.get_data(as_text=True))"
        # Will be different when reading the results from another application

        return api_response, api_error_flag

common_function_aliases = {
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
    'gmtc': 'get_missing_title_cast',
    'get_title_images_by_persons': 'get_title_images_by_persons',
    'gtibp': 'get_title_images_by_persons',
    'get_all_movies': 'get_all_movies',
    'gam': 'get_all_movies',
    'get_all_persons': 'get_all_persons',
    'gap': 'get_all_persons',
    'reconcile_movies_against_full_list': 'reconcile_movies_against_full_list',
    'rmafl': 'reconcile_movies_against_full_list',
    'reconcile_persons_against_full_list': 'reconcile_persons_against_full_list',
    'rpafl': 'reconcile_persons_against_full_list'
}

api_function_aliases = {
    
}

@app.route('/request', methods=['POST'])
def trigger_function_from_api():

    lock.acquire()

    try:
        per_run_initializations()

        params = request.args.to_dict()

        function_aliases = common_function_aliases.copy()
        function_aliases.update(api_function_aliases)

        # function = params.get('function_name')
        kwargs = params.get('kwargs', {})
        function = kwargs.get('function', None)

        function_lookup = function_aliases.get(function, function)

        if function and not function_lookup:
            valid_function_values = set(function_aliases.values())
            valid_functions_string = ''.join([f'\n--{value}' for value in valid_function_values])
            print(f'Invalid function specified.  Valid values are:{valid_functions_string}')
            return {'error': f'Unknown function: "{function}"'}, 400
        elif function and function_lookup:
            function = function_lookup

        original_language = kwargs.get('original_language', None)
        min_runtime = kwargs.get('min_runtime', None)
        adult_content_flag = kwargs.get('adult_content_flag', None)
        skip_loaded_titles = kwargs.get('skip_loaded_titles', False)
        backdrop_flag = kwargs.get('download_backdrops', False)
        poster_flag = kwargs.get('download_posters', False)
        logo_flag = kwargs.get('download_logos', False)
        search_terms = kwargs.get('search_terms', [])
        person_id_list = kwargs.get('person_ids', [])
        row_limit = kwargs.get('row_limit', None)
        time_window = kwargs.get('time_window', None)

        call_function(
            function,
            original_language=original_language,
            min_runtime=min_runtime,
            adult_content_flag=adult_content_flag,
            skip_loaded_titles=skip_loaded_titles,
            backdrop_flag=backdrop_flag,
            poster_flag=poster_flag,
            logo_flag=logo_flag,
            search_terms=search_terms,
            person_id_list=person_id_list,
            row_limit=row_limit,
            time_window=time_window
        )
        
        api_response, api_error_flag = build_api_response()
        return api_response
    
    except Exception as e:
        api_response, api_error_flag = build_api_response()
        return api_response
    
    finally:
        lock.release()

cli_function_aliases = {
    'api_listener': 'api_listener'
}

def parse_command_run_arguments():
    """Interprets the arguments passed into the command line to run the correct function"""
    global local_db, movie_data, person_data, image_data, current_time, current_time_string, current_time_log
    # import local_db_handler, movie_handler, person_handler, image_handler

    # current_time, current_time_string, current_time_log = dtu.get_current_timestamp()

    parser = argparse.ArgumentParser(description='Retrieve movie data')

    function_aliases = cli_function_aliases.copy()
    function_aliases.update(common_function_aliases)

    parser.add_argument('function', choices=function_aliases.keys(), help='Function to call')
    parser.add_argument('-host', '--api_host', type=str, help='Hostname for Flask API listener')
    parser.add_argument('-p', '--port', type=str, help='Port for Flask API listener')
    parser.add_argument('-lang', '--original_language', type=str, help='Original language')
    parser.add_argument('-rt', '--min_runtime', type=int, help='Minimum runtime in minutes')
    parser.add_argument('-adult', '--adult_content_flag', type=str, help='Adult content flag, accepts "include", "exclude" and "only"')
    parser.add_argument('-skip', '--skip_loaded_titles', action='store_true', help='Skip pulling titles already loaded')
    parser.add_argument('-search', '--search_terms', nargs='+', type=str, help='List of movie title keywords to search for')
    parser.add_argument('-pid', '--person_ids', nargs='+', type=int, help='List of person TMDB IDs')
    parser.add_argument('-rl', '--row_limit', type=int, help='Limit the number of rows returned')
    parser.add_argument('-tw', '--time_window', type=str, help='Time window for "Trending": Accepts "day" or "week"')
    parser.add_argument('-db', '--download_backdrops', action='store_true', help='Download backdrop images, if available')
    parser.add_argument('-dp', '--download_posters', action='store_true', help='Download poster images, if available')
    parser.add_argument('-dl', '--download_logos', action='store_true', help='Download logo images, if available')
    
    if not DEBUGGING_MODE:
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
        # args = argparse.Namespace(
        #     function='get_title_images_by_persons',
        #     person_ids=[1350834],
        #     skip_loaded_titles=True,
        #     adult_content_flag='o',
        #     download_backdrops=True,
        #     download_posters=True
        # )

    # # function = args.function
    function = function_aliases.get(args.function, args.function)
    host = getattr(args, 'api_host', None)
    port = getattr(args, 'port', None)
    original_language = getattr(args, 'original_language', None)
    min_runtime = getattr(args, 'min_runtime', None)
    adult_content_flag = getattr(args, 'adult_content_flag', None)
    skip_loaded_titles = getattr(args, 'skip_loaded_titles', False)
    backdrop_flag = getattr(args, 'download_backdrops', False)
    poster_flag = getattr(args, 'download_posters', False)
    logo_flag = getattr(args, 'download_logos', False)
    search_terms = getattr(args, 'search_terms', None)
    person_id_list = getattr(args, 'person_ids', None)
    row_limit = getattr(args, 'row_limit', None)
    time_window = getattr(args, 'time_window', None)

    if function:
        call_function(
                function,
                host=host,
                port=port,
                original_language=original_language,
                min_runtime=min_runtime,
                adult_content_flag=adult_content_flag,
                skip_loaded_titles=skip_loaded_titles,
                backdrop_flag=backdrop_flag,
                poster_flag=poster_flag,
                logo_flag=logo_flag,
                search_terms=search_terms,
                person_id_list=person_id_list,
                row_limit=row_limit,
                time_window=time_window)
    else:
        valid_function_values = set(function_aliases.values())
        valid_functions_string = ''.join([f'\n--{value}' for value in valid_function_values])
        print(f'Invalid function specified.  Valid values are:{valid_functions_string}')
        return

def call_function(
            function,
			host=None,
			port=None,
            original_language=None,
            min_runtime=None,
            adult_content_flag=None,
            skip_loaded_titles=None,
            backdrop_flag=None,
            poster_flag=None,
            logo_flag=None,
            search_terms=None,
            person_id_list=None,
            row_limit=None,
            time_window=None
        ):
    
    import local_db_handler, movie_handler, person_handler, image_handler
    global local_db, movie_data, person_data, image_data, current_time, current_time_string, current_time_log, function_aliases
    
    # function = function_aliases.get(function, function)
    
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
    
    if min_runtime:
        try:
            min_runtime = int(min_runtime)
        except Exception:
            min_runtime = None
    
    if skip_loaded_titles:
        skip_loaded_titles = string_utils.string_to_bool(skip_loaded_titles)
        if not isinstance(skip_loaded_titles, bool):
            skip_loaded_titles = False
            
    if row_limit:
        try:
            row_limit = int(row_limit)
        except Exception:
            row_limit = None
    
    if time_window:
        if time_window.lower() in ['day', 'd']:
            time_window = 'day'
        elif time_window.lower() in ['week', 'w']:
            time_window = 'week'
            
    if backdrop_flag:
        backdrop_flag = string_utils.string_to_bool(backdrop_flag)
        if not isinstance(backdrop_flag, bool):
            backdrop_flag = False
            
    if poster_flag:
        poster_flag = string_utils.string_to_bool(poster_flag)
        if not isinstance(poster_flag, bool):
            poster_flag = False
            
    if logo_flag:
        logo_flag = string_utils.string_to_bool(logo_flag)
        if not isinstance(logo_flag, bool):
            logo_flag = False

    local_db = local_db_handler.LocalDB(
        engine,
        global_adult_content_flag,
        loaded_titles_sql=loaded_titles_sql,
        loaded_title_cast_sql=loaded_title_cast_sql,
        loaded_persons_sql=loaded_persons_sql,
        loaded_title_images_sql=loaded_title_images_sql,
        favorite_persons_sql=favorite_persons_sql,
        search_terms_sql=search_terms_sql,
        title_images_by_favorite_persons_sql=title_images_by_favorite_persons_sql,
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
    
    if function == 'api_listener':
        run_flask_app(host=host, port=port)
    elif function == 'display_missing_counts':
        display_missing_counts(local_db)
        return
    elif function == 'get_movies_updated_yesterday':
        print_job_start()
        get_movies_updated_yesterday(original_language=original_language, min_runtime=min_runtime, adult_content_flag=adult_content_flag)
        print_job_end()
    elif function == 'get_movies_by_favorite_actor':
        print_job_start()
        get_movies_by_favorite_actor(person_id_list=person_id_list, adult_content_flag=adult_content_flag, skip_loaded_titles=skip_loaded_titles, row_limit=row_limit)
        print_job_end()
    elif function == 'get_movies_by_search_terms':
        print_job_start()
        get_movies_by_search_terms(search_terms=search_terms, original_language=original_language, adult_content_flag=adult_content_flag, skip_loaded_titles=skip_loaded_titles, row_limit=row_limit)
        print_job_end()
    elif function == 'get_trending_movies':
        print_job_start()
        get_trending_movies(time_window=time_window, original_language=original_language, skip_loaded_titles=skip_loaded_titles, row_limit=row_limit)
        print_job_end()
    elif function == 'get_missing_title_keywords':
        print_job_start()
        display_missing_counts(local_db)
        get_missing_title_keywords(adult_content_flag=adult_content_flag, row_limit=row_limit)
        print_job_end()
    elif function == 'get_missing_persons':
        print_job_start()
        display_missing_counts(local_db)
        get_missing_persons(adult_content_flag=adult_content_flag, row_limit=row_limit)
        print_job_end()
    elif function == 'get_missing_title_cast':
        print_job_start()
        display_missing_counts(local_db)
        get_missing_title_cast(adult_content_flag=adult_content_flag, row_limit=row_limit)
        print_job_end()
    elif function == 'get_title_images_by_persons':
        print_job_start()
        get_title_images_by_persons(person_id_list=person_id_list, skip_loaded_titles=skip_loaded_titles, adult_content_flag=adult_content_flag, row_limit=row_limit, backdrop_flag=backdrop_flag, poster_flag=poster_flag, logo_flag=logo_flag)
        print_job_end()
    elif function == 'get_all_movies':
        print_job_start()
        get_all_movies()
        print_job_end()
    elif function == 'get_all_persons':
        print_job_start()
        get_all_persons()
        print_job_end()
    elif function == 'reconcile_movies_against_full_list':
        print_job_start()
        reconcile_movies_against_full_list()
        print_job_end()
    elif function == 'reconcile_persons_against_full_list':
        print_job_start()
        reconcile_persons_against_full_list()
        print_job_end()

def display_missing_counts(local_db):
    # print(f'Global Adult Content Flag: {global_adult_content_flag}')
    print(f'Session Adult Content Flag: {local_db.adult_content_flag}')
    print(f'Missing Cast: {len(local_db.titles_missing_cast):,}')
    print(f'Missing Keywords:  {len(local_db.titles_missing_keywords):,}')
    print(f'Missing Persons:  {len(local_db.persons_missing):,}')

def get_all_movies():
    
    # suffix = datetime.datetime.now().strftime("%Y-%m-%d")
    suffix = current_time_string
    
    df_titles = movie_data.get_full_movie_list()

    if len(df_titles) > 0:
        movie_data.process_title_data_subset(df_titles, suffix)

def reconcile_movies_against_full_list():
    
    suffix = current_time_string
    
    df_titles = movie_data.get_full_movie_list()

    missing_titles = [tmdb_id for tmdb_id in local_db.loaded_titles if tmdb_id not in df_titles['tmdb_id'].values]
    missing_titles = [title for title in missing_titles if title not in local_db.loaded_titles_adult]

    handle_missing_data(suffix, tmdb_id_list=missing_titles)

def get_all_persons():
    
    # suffix = datetime.datetime.now().strftime("%Y-%m-%d")
    suffix = current_time_string
    
    df_persons = person_data.get_full_person_list()

    if len(df_persons) > 0:
        person_data.process_person_data_subset(df_persons, suffix)

def reconcile_persons_against_full_list():
    
    suffix = current_time_string
    
    df_persons = person_data.get_full_person_list()

    missing_persons = [person_id for person_id in local_db.loaded_persons if person_id not in df_persons['person_id'].values]
    missing_persons = [person for person in missing_persons if person not in local_db.loaded_persons_adult]

    handle_missing_data(suffix, person_id_list=missing_persons)

def handle_missing_data(suffix=None, tmdb_id_list=[], person_id_list=[]):
    
    if not suffix:
        suffix = current_time_string
    
    if not tmdb_id_list:
        tmdb_id_list = local_db.error_tmdb_id_list
    
    if tmdb_id_list:
        df_removed_titles = movie_data.check_title_exists(tmdb_id_list)

        if len(df_removed_titles) > 0:
            movie_data.process_removed_titles(df_removed_titles, suffix)
    
    if not person_id_list:
        person_id_list = local_db.error_person_id_list
    
    if person_id_list:
        df_removed_persons = person_data.check_person_exists(person_id_list)

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

    handle_missing_data()

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
        df_titles = movie_data.get_title_data(tmdb_id_list, original_language=original_language)
        
        if len(df_titles) > 0:
            movie_data.process_title_data(df_titles, suffix)
        else:
            print('No New Titles')
    else:
        print('No New Titles')

    handle_missing_data(suffix)

def get_title_images_by_persons(person_id_list, suffix=None, skip_loaded_titles=True, adult_content_flag=None, row_limit=None, backdrop_flag=False, poster_flag=False, logo_flag=False):
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
        ids_to_skip = local_db.loaded_title_images
        tmdb_id_list = [item for item in tmdb_id_list if item not in ids_to_skip]
    else:
        ids_to_skip = []
    
    if row_limit and tmdb_id_list and len(tmdb_id_list) >= row_limit:
        tmdb_id_list = tmdb_id_list[:row_limit]

    # If no image type arguments are given, retrieve all types
    if backdrop_flag == False and poster_flag == False and logo_flag == False:
        backdrop_flag, poster_flag, logo_flag = True
    # else:
    #     if backdrop_flag is None:
    #         backdrop_flag = False
    #     if poster_flag is None:
    #         poster_flag = False
    #     if logo_flag is None:
    #         logo_flag = False
    
    if tmdb_id_list:
        df_images = image_data.get_image_data(tmdb_id_list, ids_to_skip, backdrop_flag, poster_flag, logo_flag)

        if len(df_images) > 0:
            image_data.process_images(df_images, suffix)
            missing_tmdb_ids = [tmdb_id for tmdb_id in df_images['tmdb_id'] if tmdb_id not in local_db.loaded_titles]
            if len(missing_tmdb_ids) > 0:
                df_titles = movie_data.get_title_data(missing_tmdb_ids)
                if len(df_titles) > 0:
                    movie_data.process_title_data(df_titles, suffix=current_time_string)
        else:
            print('No images to process')
    else:
        print('No images to process')

    handle_missing_data(suffix)

def get_missing_title_keywords(tmdb_id_list=[], adult_content_flag=None, row_limit=None):
    
    suffix = current_time_string

    if not adult_content_flag:
        if global_adult_content_flag:
            adult_content_flag = global_adult_content_flag
        else:
            adult_content_flag = 'exclude'
    
    if not tmdb_id_list:
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

def get_missing_persons(person_id_list=[], adult_content_flag=None, row_limit=None):
    
    suffix = current_time_string

    if not adult_content_flag:
        if global_adult_content_flag:
            adult_content_flag = global_adult_content_flag
        else:
            adult_content_flag = 'exclude'

    if not person_id_list:
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

def get_missing_title_cast(tmdb_id_list=[], adult_content_flag=None, row_limit=None):
    
    suffix = current_time_string

    if not adult_content_flag:
        if global_adult_content_flag:
            adult_content_flag = global_adult_content_flag
        else:
            adult_content_flag = 'exclude'
    
    if not tmdb_id_list:
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

def create_title_image_html(tmdb_id_list=[], html_path=None, html_name=None, backdrop_required_flag=False):
    
    image_data.create_title_image_html(tmdb_id_list=tmdb_id_list, html_path=html_path, html_name=html_name, backdrop_required_flag=backdrop_required_flag)

def create_image_html_by_person(person_id_list, adult_content_flag=None, backdrop_required_flag=False):
    
    if not adult_content_flag:
        adult_content_flag = global_adult_content_flag
    
    for person in person_id_list:
        single_person_id_list = [person]
        person_details = person_data.get_person_data(single_person_id_list)
        person_name = str(person_details['person_name'].str.lower().str.replace(" ", "_").iat[0])
        tmdb_id_list = person_data.get_titles_by_person(single_person_id_list, adult_content_flag=adult_content_flag)
    
        # df_title_images = local_db.title_images_by_favorite_persons[local_db.title_images_by_favorite_persons['tmdb_id'].isin(tmdb_id_list)]

        create_title_image_html(tmdb_id_list, html_name=(person_name + '.html'), backdrop_required_flag=backdrop_required_flag)

def print_job_start():
    global current_time

    print(f'Job Start: {current_time}')

def print_job_end():
    global current_time
    job_end_current_time, job_end_current_time_string, job_end_current_time_log = dtu.get_current_timestamp()
    days, hours, minutes, seconds, job_duration = dtu.get_duration(current_time, job_end_current_time)
    print(f'Job End: {job_end_current_time}\nTotal Duration: {job_duration}')

def per_run_initializations():
    global local_db, movie_data, person_data, image_data

    local_db = local_db_handler.LocalDB(
        engine,
        global_adult_content_flag,
        loaded_titles_sql=loaded_titles_sql,
        loaded_title_cast_sql=loaded_title_cast_sql,
        loaded_persons_sql=loaded_persons_sql,
        loaded_title_images_sql=loaded_title_images_sql,
        favorite_persons_sql=favorite_persons_sql,
        search_terms_sql=search_terms_sql,
        title_images_by_favorite_persons_sql=title_images_by_favorite_persons_sql,
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


#%%
os.environ['PYDEVD_WARN_EVALUATION_TIMEOUT'] = '2000'
# run_from_command_line = os.getenv("PYTHON_ISATTY", "True").lower() == "true" and sys.stdin.isatty()
# if 'KUBERNETES_SERVICE_HOST' in os.environ:
#     run_from_command_line = True

DEBUGGING_MODE = False
if 'VSCODE_PID' in os.environ or sys.gettrace() is not None:
    DEBUGGING_MODE = True
if 'KUBERNETES_SERVICE_HOST' in os.environ:
    DEBUGGING_MODE = False
# if DEBUGGING_MODE:
#     print(f'DEBUGGING_MODE: {DEBUGGING_MODE}')

# # current_dir = os.getcwd()
# # current_dir = os.path.dirname(os.path.abspath(__file__))
# current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
# # print(current_dir)
# # project_dir = current_dir

# api_result = None
# api_error_flag = None
# api_error_message = None

# s3_config_path = Path(current_dir).parent.parent / 'data' / 'Config'
# config_path = None
# if s3_config_path.exists():
#     ini_files = [entry for entry in os.scandir(s3_config_path) if entry.is_file() and entry.name.endswith('.ini')]
#     if len(ini_files) >= 3:
#         config_path = Path(current_dir).parent.parent / 'data' / 'Config'
# if not config_path:
#     config_path = Path(current_dir).parent.parent / 'config'
# app_config_path = config_path / "app_config.ini"
# connection_config_path = config_path / "connections_config.ini"
# output_file_config_path = config_path / "output_files_config.ini"
# sql_queries_config_path = config_path / "sql_queries"

# # read_app_config_settings
# (
#     output_path, 
#     images_path, 
#     # log_file_path, 
#     # password_method, 
#     # password_access_key, 
#     # password_secret_key, 
#     # password_endpoint_url, 
#     # password_region_name, 
#     # password_password_path, 
#     # read_chunk_size, 
#     # archive_flag, 
#     # logging_flag, 
#     # log_archive_expire_days, 
#     global_original_language, 
#     global_adult_content_flag
#     ) = conn.read_app_config_settings(app_config_path)

# if global_adult_content_flag and global_adult_content_flag not in ('include', 'exclude', 'only'):
#     global_adult_content_flag = 'exclude'

# db_target_config = 'target_connection'
# # read_connection_config_settings
# (
#     connect_type, 
#     server_address, 
#     server_port, 
#     database_name, 
#     # schema, 
#     db_password_method, 
#     db_password_access_key, 
#     db_password_secret_key, 
#     db_password_endpoint_url, 
#     db_password_region_name, 
#     db_password_password_path, 
#     db_user_name, 
#     db_secret_key
#     ) = conn.read_connection_config_settings(connection_config_path, db_target_config)

# db_password = None

# if 'NEXUS_TDR_TARGET_DB_PASSWORD' in os.environ:
#     db_password = os.environ['NEXUS_TDR_TARGET_DB_PASSWORD']
# else:
#     db_password = pw.get_password(
#         db_password_method, 
#         password_key=db_secret_key, 
#         account_name=db_user_name, 
#         access_key=db_password_access_key, 
#         secret_key=db_password_secret_key, 
#         endpoint_url=db_password_endpoint_url, 
#         region_name=db_password_region_name, 
#         password_path=db_password_password_path)

# try:
#     engine = build_engine(connect_type, server_address, server_port, database_name, db_user_name, db_password)#, schema)
# except Exception as e:
#     engine = None

# api_source_config = 'tmdb_api_connection'
# # read_connection_config_settings
# (
#     api_password_method, 
#     api_password_access_key, 
#     api_password_secret_key, 
#     api_password_endpoint_url, 
#     api_password_region_name, 
#     api_password_password_path, 
#     api_user_name, 
#     api_secret_key
#     ) = conn.read_connection_config_settings(connection_config_path, api_source_config)
# # api_key = pw.get_password(api_password_method, password_key=api_secret_key, account_name=api_user_name)

# api_key = None

# if 'NEXUS_TMDB_API_KEY' in os.environ:
#     api_key = os.environ['NEXUS_TMDB_API_KEY']
# else:
#     api_key = pw.get_password(
#         api_password_method, 
#         password_key=api_secret_key, 
#         account_name=api_user_name, 
#         access_key=api_password_access_key, 
#         secret_key=api_password_secret_key, 
#         endpoint_url=api_password_endpoint_url, 
#         region_name=api_password_region_name, 
#         password_path=api_password_password_path)

# # read_sql_queries
# (
#     loaded_titles_sql, 
#     loaded_title_cast_sql, 
#     loaded_persons_sql, 
#     loaded_title_images_sql, 
#     favorite_persons_sql, 
#     search_terms_sql, 
#     title_images_by_favorite_persons_sql, 
#     titles_missing_cast_sql, 
#     titles_missing_keywords_sql, 
#     persons_missing_sql
#     ) = misc.read_sql_queries(sql_queries_config_path)
# # loaded_titles_sql, loaded_title_cast_sql, loaded_persons_sql, loaded_title_images_sql, favorite_persons_sql, search_terms_sql, titles_missing_cast_sql, titles_missing_keywords_sql, persons_missing_sql = conn.read_sql_queries(sql_queries_config_path)

# # read_output_file_flags
# (
#     output_titles_flag, 
#     output_title_genres_flag, 
#     output_genres_flag, 
#     output_title_spoken_languages_flag, 
#     output_spoken_languages_flag, 
#     output_title_production_countries_flag, 
#     output_production_countries_flag, 
#     output_title_production_companies_flag, 
#     output_production_companies_flag, 
#     output_title_collections_flag, 
#     output_collections_flag, 
#     output_title_keywords_flag, 
#     output_keywords_flag, 
#     output_persons_flag, 
#     output_person_aka_flag, 
#     output_title_cast_flag, 
#     output_title_images_flag, 
#     output_person_removed_flag, 
#     output_title_removed_flag
#     ) = conn.read_output_file_flags(output_file_config_path)

# current_time, current_time_string, current_time_log = dtu.get_current_timestamp()

my_settings = settings.Settings()

local_db = None
movie_data = None
person_data = None
image_data = None

# local_db = local_db_handler.LocalDB(
#     engine,
#     global_adult_content_flag,
#     loaded_titles_sql=loaded_titles_sql,
#     loaded_title_cast_sql=loaded_title_cast_sql,
#     loaded_persons_sql=loaded_persons_sql,
#     loaded_title_images_sql=loaded_title_images_sql,
#     favorite_persons_sql=favorite_persons_sql,
#     search_terms_sql=search_terms_sql,
#     title_images_by_favorite_persons_sql=title_images_by_favorite_persons_sql,
#     titles_missing_cast_sql=titles_missing_cast_sql,
#     titles_missing_keywords_sql=titles_missing_keywords_sql,
#     persons_missing_sql=persons_missing_sql
# )
# movie_data = movie_handler.MovieData(
#     api_key, 
#     local_db, 
#     output_path, 
#     output_titles_flag, 
#     output_title_genres_flag, 
#     output_genres_flag, 
#     output_title_spoken_languages_flag, 
#     output_spoken_languages_flag, 
#     output_title_production_countries_flag, 
#     output_production_countries_flag, 
#     output_title_production_companies_flag, 
#     output_production_companies_flag, 
#     output_title_collections_flag, 
#     output_collections_flag, 
#     output_title_keywords_flag, 
#     output_keywords_flag, 
#     output_title_removed_flag
# )
# person_data = person_handler.PersonData(
#     api_key, 
#     local_db, 
#     output_path, 
#     output_persons_flag, 
#     output_person_aka_flag, 
#     output_title_cast_flag, 
#     output_person_removed_flag)
# image_data = image_handler.ImageData(
#     api_key, 
#     local_db, 
#     output_path, 
#     images_path, 
#     output_title_images_flag)

#%%

if __name__ == '__main__':
    if not DEBUGGING_MODE:
        per_run_initializations()
        parse_command_run_arguments()
    else:
        import local_db_handler, movie_handler, person_handler, image_handler
        
        # current_time, current_time_string, current_time_log = dtu.get_current_timestamp()

        # parse_command_run_arguments()
        
        print_job_start()

        per_run_initializations(my_settings)

        # local_db = local_db_handler.LocalDB(
        #     engine,
        #     global_adult_content_flag,
        #     loaded_titles_sql=loaded_titles_sql,
        #     loaded_title_cast_sql=loaded_title_cast_sql,
        #     loaded_persons_sql=loaded_persons_sql,
        #     loaded_title_images_sql=loaded_title_images_sql,
        #     favorite_persons_sql=favorite_persons_sql,
        #     search_terms_sql=search_terms_sql,
        #     title_images_by_favorite_persons_sql=title_images_by_favorite_persons_sql,
        #     titles_missing_cast_sql=titles_missing_cast_sql,
        #     titles_missing_keywords_sql=titles_missing_keywords_sql,
        #     persons_missing_sql=persons_missing_sql
        # )
        # movie_data = movie_handler.MovieData(
        #     api_key, 
        #     local_db, 
        #     output_path, 
        #     output_titles_flag, 
        #     output_title_genres_flag, 
        #     output_genres_flag, 
        #     output_title_spoken_languages_flag, 
        #     output_spoken_languages_flag, 
        #     output_title_production_countries_flag, 
        #     output_production_countries_flag, 
        #     output_title_production_companies_flag, 
        #     output_production_companies_flag, 
        #     output_title_collections_flag, 
        #     output_collections_flag, 
        #     output_title_keywords_flag, 
        #     output_keywords_flag, 
        #     output_title_removed_flag
        # )
        # person_data = person_handler.PersonData(
        #     api_key, 
        #     local_db, 
        #     output_path, 
        #     output_persons_flag, 
        #     output_person_aka_flag, 
        #     output_title_cast_flag, 
        #     output_person_removed_flag)
        # image_data = image_handler.ImageData(
        #     api_key, 
        #     local_db, 
        #     output_path, 
        #     images_path, 
        #     output_title_images_flag)

        # movie_data.get_title_data([1089679])

        # api_result = {'key1':'value1','key2':'value2'}
        # api_error_flag = False
        # build_api_response()
        
        # get_all_movies()
        # get_all_persons()
        # reconcile_movies_against_full_list()
        # reconcile_persons_against_full_list()

        # get_title_images_by_persons(local_db.favorite_persons, suffix=None, skip_loaded_titles=True, adult_content_flag='only', row_limit=None, backdrop_flag=True, poster_flag=True, logo_flag=True)
        # get_title_images_by_persons([], suffix=None, skip_loaded_titles=True, adult_content_flag='only', row_limit=None, backdrop_flag=True, poster_flag=True, logo_flag=True)
        # create_image_html_by_person(local_db.favorite_persons)
        # create_image_html_by_person(local_db.favorite_persons, backdrop_required_flag=True)

        # print(local_db.functioning_engine_message)
        # create_title_image_html()
        # display_missing_counts(local_db)

        # get_movies_by_search_terms(original_language='en', skip_loaded_titles=True)#, row_limit=12)
        # get_trending_movies(time_window='week', original_language='en', skip_loaded_titles=True, row_limit=2_000)

        # get_movies_by_favorite_actor([74252], adult_content_flag=True, ids_to_skip=loaded_titles)
        # get_movies_by_favorite_actor(ids_to_skip=loaded_titles)

        # get_movies_updated_yesterday(original_language='en', adult_content_flag='include')
        # get_movies_updated_yesterday(original_language='en')#, adult_content_flag='only')

        # movie_data.get_movie_discover_data(adult_content_flag='only')

        # movie_data.get_title_data([1089679])
        # get_movies_by_search_terms(['christmas'], original_language='en')

        # get_title_images_by_persons([1912793], skip_loaded_titles=False)

        # get_missing_persons(current_time_string, [1535848, 1426252, 135660, 143070, 136331, 76575, 932319, 1056772, 2953862, 997483, 591313])
        # get_missing_title_cast()
        # get_missing_persons()
        # get_missing_title_keywords()

        # tmdb_list = person_data.get_titles_by_person([1535848, 1426252, 135660, 143070, 136331, 76575, 932319, 1056772, 2953862, 997483, 591313], local_db.loaded_title_cast)

        # person_data.get_title_cast_data_by_movie(tmdb_list)

        # person_data.get_title_cast_data_by_movie([1064688, 364385])

        # get_missing_title_cast(row_limit=100)
        # get_missing_persons(row_limit=1_000)
        # print(len(local_db.loaded_persons))
        # get_missing_persons([1158])
        # print(len(local_db.loaded_persons))

        # get_missing_title_keywords(row_limit=200)
        # get_missing_persons()

        # get_movies_by_favorite_actor(adult_content_flag=True, ids_to_skip=local_db.loaded_titles)

        print_job_end()

        #%%