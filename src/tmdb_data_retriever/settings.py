# import sys
import os
from pathlib import Path
import glob
# import argparse
# import dateparser
# import datetime
# from flask import Flask, request, make_response, jsonify
# import json
# from threading import Lock
import inspect
from nexus_utils.database_utils import build_engine
from nexus_utils import password_utils as pw
from nexus_utils import datetime_utils as dtu
from nexus_utils import config_utils as cr
# from nexus_utils import string_utils
# import utils.connection_utils as conn
# from src.tmdb_data_retriever.utils import connection_utils as conn
# import utils.misc_utils as misc
# from src.tmdb_data_retriever.utils import misc_utils as misc


class Settings:
    def __init__(self):

        # self.api_result = []
        # self.api_error_flag = None
        # self.api_error_message = None
        current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        self.config_path = None
        s3_config_path = Path(current_dir).parent.parent / 'data' / 'Config'

        if s3_config_path.exists():
            ini_files = [entry for entry in os.scandir(s3_config_path) if entry.is_file() and entry.name.endswith('.ini')]
            if len(ini_files) >= 3:
                self.config_path = Path(current_dir).parent.parent / 'data' / 'Config'
        if not self.config_path:
            self.config_path = Path(current_dir).parent.parent / 'config'
        self.config_path = os.getenv('NEXUS_TMDB_CONFIG_PATH', self.config_path)
        
        app_config_path = self.config_path / "app_config.ini"
        app_config_path = os.getenv('NEXUS_TMDB_CONFIG_PATH', app_config_path)
        connection_config_path = self.config_path / "connections_config.ini"
        connection_config_path = os.getenv('NEXUS_TMDB_CONNECTION_CONFIG_PATH', connection_config_path)
        output_file_config_path = self.config_path / "output_files_config.ini"
        output_file_config_path = os.getenv('NEXUS_TMDB_OUTPUT_FILES_CONFIG_PATH', output_file_config_path)
        sql_queries_config_path = self.config_path / "sql_queries"
        sql_queries_config_path = os.getenv('NEXUS_TMDB_SQL_QUERIES_CONFIG_PATH', sql_queries_config_path)

        # read_app_config_settings
        (
            self.output_path, 
            self.images_path, 
            # log_file_path, 
            # password_method, 
            # password_access_key, 
            # password_secret_key, 
            # password_endpoint_url, 
            # password_region_name, 
            # password_password_path, 
            # read_chunk_size, 
            # archive_flag, 
            # logging_flag, 
            # log_archive_expire_days, 
            self.global_original_language, 
            self.global_adult_content_flag
            ) = self.read_app_config_settings(app_config_path)

        if self.global_adult_content_flag and self.global_adult_content_flag not in ('include', 'exclude', 'only'):
            self.global_adult_content_flag = 'exclude'

        if not self.global_adult_content_flag:
            self.global_adult_content_flag = 'exclude'

        self.output_path = os.getenv('NEXUS_TMDB_FILE_OUTPUT_PATH', self.output_path)
        self.images_path = os.getenv('NEXUS_TMDB_IMAGES_OUTPUT_PATH', self.images_path)
        
        db_target_config = 'target_connection'
        # read_connection_config_settings
        (
            connect_type, 
            server_address, 
            server_port, 
            database_name, 
            # schema, 
            db_password_method, 
            db_password_access_key, 
            db_password_secret_key, 
            db_password_endpoint_url, 
            db_password_region_name, 
            db_password_password_path, 
            db_user_name, 
            db_secret_key
            ) = self.read_connection_config_settings(connection_config_path, db_target_config)

        db_password = None

        if not os.getenv('NEXUS_TMDB_TARGET_DB_PASSWORD'):
            db_password = pw.get_password(
                db_password_method, 
                password_key=db_secret_key, 
                account_name=db_user_name, 
                access_key=db_password_access_key, 
                secret_key=db_password_secret_key, 
                endpoint_url=db_password_endpoint_url, 
                region_name=db_password_region_name, 
                password_path=db_password_password_path)

        db_password = os.getenv('NEXUS_TMDB_TARGET_DB_PASSWORD', db_password)
        
        try:
            self.engine = build_engine(connect_type, server_address, server_port, database_name, db_user_name, db_password)#, schema)
        except Exception as e:
            self.engine = None

        api_source_config = 'tmdb_api_connection'
        # read_connection_config_settings
        (
            api_password_method, 
            api_password_access_key, 
            api_password_secret_key, 
            api_password_endpoint_url, 
            api_password_region_name, 
            api_password_password_path, 
            api_user_name, 
            api_secret_key
            ) = self.read_connection_config_settings(connection_config_path, api_source_config)
        # api_key = pw.get_password(api_password_method, password_key=api_secret_key, account_name=api_user_name)

        self.api_key = None

        if not os.getenv('NEXUS_TMDB_API_KEY'):
            self.api_key = pw.get_password(
                api_password_method, 
                password_key=api_secret_key, 
                account_name=api_user_name, 
                access_key=api_password_access_key, 
                secret_key=api_password_secret_key, 
                endpoint_url=api_password_endpoint_url, 
                region_name=api_password_region_name, 
                password_path=api_password_password_path)

        self.api_key = os.getenv('NEXUS_TMDB_API_KEY', self.api_key)

        # read_sql_queries
        (
            self.loaded_titles_sql, 
            self.loaded_title_cast_sql, 
            self.loaded_persons_sql, 
            self.loaded_title_images_sql, 
            self.favorite_persons_sql, 
            self.search_terms_sql, 
            self.title_images_by_favorite_persons_sql, 
            self.titles_missing_cast_sql, 
            self.titles_missing_keywords_sql, 
            self.persons_missing_sql
            ) = self.read_sql_queries(sql_queries_config_path)
        # loaded_titles_sql, loaded_title_cast_sql, loaded_persons_sql, loaded_title_images_sql, favorite_persons_sql, search_terms_sql, titles_missing_cast_sql, titles_missing_keywords_sql, persons_missing_sql = self.read_sql_queries(sql_queries_config_path)

        # read_output_file_flags
        (
            self.output_titles_flag, 
            self.output_genres_flag, 
            self.output_title_spoken_languages_flag, 
            self.output_spoken_languages_flag, 
            self.output_title_production_countries_flag, 
            self.output_production_countries_flag, 
            self.output_title_production_companies_flag, 
            self.output_production_companies_flag, 
            self.output_title_collections_flag, 
            self.output_collections_flag, 
            self.output_title_keywords_flag, 
            self.output_keywords_flag, 
            self.output_persons_flag, 
            self.output_person_aka_flag, 
            self.output_title_cast_flag, 
            self.output_title_images_flag, 
            self.output_person_removed_flag, 
            self.output_title_removed_flag, 
            self.output_title_genres_flag, 
            ) = self.read_output_file_flags(output_file_config_path)

        self.current_time, self.current_time_string, self.current_time_log = dtu.get_current_timestamp()

        # local_db = None
        # movie_data = None
        # person_data = None
        # image_data = None

    # def check_for_local_config(self, config_path):
    #     path_obj = Path(config_path)
    #     local_path = path_obj.with_name(path_obj.stem + "_local" + path_obj.suffix)
        
    #     if local_path.exists():
    #         return str(local_path)
    #     elif path_obj.exists():
    #         return str(path_obj)
    #     else:
    #         return None

    def read_app_config_settings(self, _app_config_path):
        """Read app configuration parameters"""
        # _app_config_path = check_for_local_config(_app_config_path)
        _app_config_path = Path(_app_config_path)
        app_config = cr.read_config_file(_app_config_path)  # type: ignore
        local_config_entry = 'app_settings'
        output_path = app_config[local_config_entry]['output_path']
        images_path = app_config[local_config_entry]['images_path']
        # log_file_path = app_config[local_config_entry]['log_file_path']
        # password_method = app_config[local_config_entry]['password_method']
        # password_access_key = app_config[local_config_entry]['password_access_key']
        # password_secret_key = app_config[local_config_entry]['password_secret_key']
        # password_endpoint_url = app_config[local_config_entry]['password_endpoint_url']
        # password_region_name = app_config[local_config_entry]['password_region_name']
        # password_password_path = app_config[local_config_entry]['password_password_path']
        # read_chunk_size = int(app_config[local_config_entry]['read_chunk_size'])
        # archive_flag = app_config[local_config_entry]['archive_flag'].lower() in ['t', 'true', 'y', 'yes', '1', 'on', 'archive']
        # logging_flag = app_config[local_config_entry]['logging_flag'].lower() in ['t', 'true', 'y', 'yes', '1', 'on', 'log']
        # log_archive_expire_days = int(app_config[local_config_entry]['log_archive_expire_days']),
        original_language = app_config[local_config_entry]['original_language']
        adult_content_flag = app_config[local_config_entry]['adult_content_flag'].lower()

        return (
            output_path, 
            images_path, 
            # log_file_path, 
            # password_method, 
            # password_access_key, 
            # password_secret_key, 
            # password_endpoint_url, 
            # password_region_name, 
            # password_password_path, 
            # read_chunk_size, 
            # archive_flag, 
            # logging_flag, 
            # log_archive_expire_days, 
            original_language, 
            adult_content_flag
            )

    def read_connection_config_settings(self, _connection_config_path, config_entry):
        """Read database connection configuration"""
        # _connection_config_path = check_for_local_config(_connection_config_path)
        if config_entry == 'target_connection':
            db_config = cr.read_config_file(_connection_config_path)
            connect_type = db_config[config_entry]['connect_type']
            server_address = db_config[config_entry]['server_address']
            server_port = db_config[config_entry]['server_port']
            server_name = db_config[config_entry]['server_name']
            # schema = db_config[config_entry]['schema']
            password_method = db_config[config_entry]['password_method']
            password_access_key = db_config[config_entry]['password_access_key']
            password_secret_key = db_config[config_entry]['password_secret_key']
            password_endpoint_url = db_config[config_entry]['password_endpoint_url']
            password_region_name = db_config[config_entry]['password_region_name']
            password_password_path = db_config[config_entry]['password_password_path']
            user_name = db_config[config_entry]['user_name']
            secret_key = db_config[config_entry]['secret_key']

            return (
                connect_type, 
                server_address, 
                server_port, 
                server_name, 
                # schema, 
                password_method, 
                password_access_key, 
                password_secret_key, 
                password_endpoint_url, 
                password_region_name, 
                password_password_path, 
                user_name, 
                secret_key
            )
        elif config_entry == 'tmdb_api_connection':
            db_config = cr.read_config_file(_connection_config_path)
            password_method = db_config[config_entry]['password_method']
            password_access_key = db_config[config_entry]['password_access_key']
            password_secret_key = db_config[config_entry]['password_secret_key']
            password_endpoint_url = db_config[config_entry]['password_endpoint_url']
            password_region_name = db_config[config_entry]['password_region_name']
            password_password_path = db_config[config_entry]['password_password_path']
            user_name = db_config[config_entry]['user_name']
            secret_key = db_config[config_entry]['secret_key']

            return (
                password_method, 
                password_access_key, 
                password_secret_key, 
                password_endpoint_url, 
                password_region_name, 
                password_password_path, 
                user_name, 
                secret_key
            )
        else:
            return None

    def read_output_file_flags(self, _output_files_config_path):
        """Read output files configuration"""
        db_config = cr.read_config_file(_output_files_config_path)
        config_entry = 'output_files'
        titles = db_config[config_entry].get('titles', True)
        title_genres = db_config[config_entry].get('title_genres', True)
        genres = db_config[config_entry].get('genres', True)
        title_spoken_languages = db_config[config_entry].get('title_spoken_languages', True)
        spoken_languages = db_config[config_entry].get('spoken_languages', True)
        title_production_countries = db_config[config_entry].get('title_production_countries', True)
        production_countries = db_config[config_entry].get('production_countries', True)
        title_production_companies = db_config[config_entry].get('title_production_companies', True)
        production_companies = db_config[config_entry].get('production_companies', True)
        title_collections = db_config[config_entry].get('title_collections', True)
        collections = db_config[config_entry].get('collections', True)
        title_keywords = db_config[config_entry].get('title_keywords', True)
        keywords = db_config[config_entry].get('keywords', True)
        persons = db_config[config_entry].get('persons', True)
        person_aka = db_config[config_entry].get('person_aka', True)
        title_cast = db_config[config_entry].get('title_cast', True)
        title_images = db_config[config_entry].get('title_images', True)
        title_removed = db_config[config_entry].get('title_removed', True)
        person_removed = db_config[config_entry].get('person_removed', True)

        return (
            titles, 
            title_genres, 
            genres, 
            title_spoken_languages, 
            spoken_languages, 
            title_production_countries, 
            production_countries, 
            title_production_companies, 
            production_companies, 
            title_collections, 
            collections, 
            title_keywords, 
            keywords, 
            persons, 
            person_aka, 
            title_cast, 
            title_images, 
            title_removed, 
            person_removed
        )

    def read_sql_queries(self, sql_queries_path):
        local_folder_path = sql_queries_path.with_name(sql_queries_path.name + "_local")
        
        if local_folder_path.exists() and local_folder_path.is_dir():
            sql_queries_path = local_folder_path
        
        sql_files = glob.glob(str(sql_queries_path / "*.sql"))

        sql_queries = {}

        for file_path in sql_files:
            file_name = Path(file_path).stem
            with open(file_path, "r") as file:
                query = file.read()
                sql_queries[file_name] = query

        loaded_titles_sql = sql_queries.get("loaded_titles_sql")
        loaded_title_cast_sql = sql_queries.get("loaded_title_cast_sql")
        loaded_persons_sql = sql_queries.get("loaded_persons_sql")
        loaded_title_images_sql = sql_queries.get("loaded_title_images_sql")
        favorite_persons_sql = sql_queries.get("favorite_persons_sql")
        search_terms_sql = sql_queries.get("search_terms_sql")
        title_images_by_favorite_persons_sql = sql_queries.get("title_images_by_favorite_persons_sql")
        titles_missing_cast_sql = sql_queries.get("titles_missing_cast_sql")
        titles_missing_keywords_sql = sql_queries.get("titles_missing_keywords_sql")
        persons_missing_sql = sql_queries.get("persons_missing_sql")

        return(
            loaded_titles_sql, 
            loaded_title_cast_sql, 
            loaded_persons_sql, 
            loaded_title_images_sql, 
            favorite_persons_sql, 
            search_terms_sql, 
            title_images_by_favorite_persons_sql, 
            titles_missing_cast_sql, 
            titles_missing_keywords_sql, 
            persons_missing_sql
        )

    # def read_sql_queries(self, _sql_queries_config_path):
    #     """Read sql query configuration"""
    #     # _sql_queries_config_path = check_for_local_config(_sql_queries_config_path)
    #     db_config = cr.read_config_file(_sql_queries_config_path)
    #     config_entry = 'sql_statements'
    #     loaded_titles_sql = db_config[config_entry]['loaded_titles_sql']
    #     loaded_title_cast_sql = db_config[config_entry]['loaded_title_cast_sql']
    #     loaded_persons_sql = db_config[config_entry]['loaded_persons_sql']
    #     loaded_title_images_sql = db_config[config_entry]['loaded_title_images_sql']
    #     favorite_persons_sql = db_config[config_entry]['favorite_persons_sql']
    #     search_terms_sql = db_config[config_entry]['search_terms_sql']
    #     titles_missing_cast_sql = db_config[config_entry]['titles_missing_cast_sql']
    #     titles_missing_keywords_sql = db_config[config_entry]['titles_missing_keywords_sql']
    #     persons_missing_sql = db_config[config_entry]['persons_missing_sql']

    #     return (
    #         loaded_titles_sql, 
    #         loaded_title_cast_sql, 
    #         loaded_persons_sql, 
    #         loaded_title_images_sql, 
    #         favorite_persons_sql, 
    #         search_terms_sql, 
    #         titles_missing_cast_sql, 
    #         titles_missing_keywords_sql, 
    #         persons_missing_sql
    #     )
