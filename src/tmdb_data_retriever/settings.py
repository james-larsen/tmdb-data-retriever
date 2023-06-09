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


class Settings:
    def __init__(
            self
            ):

        self.api_result = None
        self.api_error_flag = None
        self.api_error_message = None
        current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        self.config_path = None
        s3_config_path = Path(current_dir).parent.parent / 'data' / 'Config'

        if s3_config_path.exists():
            ini_files = [entry for entry in os.scandir(s3_config_path) if entry.is_file() and entry.name.endswith('.ini')]
            if len(ini_files) >= 3:
                self.config_path = Path(current_dir).parent.parent / 'data' / 'Config'
        if not self.config_path:
            self.config_path = Path(current_dir).parent.parent / 'config'
        app_config_path = self.config_path / "app_config.ini"
        connection_config_path = self.config_path / "connections_config.ini"
        output_file_config_path = self.config_path / "output_files_config.ini"
        sql_queries_config_path = self.config_path / "sql_queries"

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
            ) = conn.read_app_config_settings(app_config_path)

        if self.global_adult_content_flag and self.global_adult_content_flag not in ('include', 'exclude', 'only'):
            self.global_adult_content_flag = 'exclude'

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
            ) = conn.read_connection_config_settings(connection_config_path, db_target_config)

        db_password = None

        if 'NEXUS_TDR_TARGET_DB_PASSWORD' in os.environ:
            db_password = os.environ['NEXUS_TDR_TARGET_DB_PASSWORD']
        else:
            db_password = pw.get_password(
                db_password_method, 
                password_key=db_secret_key, 
                account_name=db_user_name, 
                access_key=db_password_access_key, 
                secret_key=db_password_secret_key, 
                endpoint_url=db_password_endpoint_url, 
                region_name=db_password_region_name, 
                password_path=db_password_password_path)

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
            ) = conn.read_connection_config_settings(connection_config_path, api_source_config)
        # api_key = pw.get_password(api_password_method, password_key=api_secret_key, account_name=api_user_name)

        self.api_key = None

        if 'NEXUS_TMDB_API_KEY' in os.environ:
            self.api_key = os.environ['NEXUS_TMDB_API_KEY']
        else:
            self.api_key = pw.get_password(
                api_password_method, 
                password_key=api_secret_key, 
                account_name=api_user_name, 
                access_key=api_password_access_key, 
                secret_key=api_password_secret_key, 
                endpoint_url=api_password_endpoint_url, 
                region_name=api_password_region_name, 
                password_path=api_password_password_path)

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
            ) = misc.read_sql_queries(sql_queries_config_path)
        # loaded_titles_sql, loaded_title_cast_sql, loaded_persons_sql, loaded_title_images_sql, favorite_persons_sql, search_terms_sql, titles_missing_cast_sql, titles_missing_keywords_sql, persons_missing_sql = conn.read_sql_queries(sql_queries_config_path)

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
            ) = conn.read_output_file_flags(output_file_config_path)

        self.current_time, self.current_time_string, self.current_time_log = dtu.get_current_timestamp()

        # local_db = None
        # movie_data = None
        # person_data = None
        # image_data = None