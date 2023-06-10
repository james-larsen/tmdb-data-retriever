from pathlib import Path
# from sqlalchemy.exc import OperationalError
# from sqlalchemy import text
from nexus_utils import config_utils as cr

# # def check_for_local_config(config_path):
# #     path_obj = Path(config_path)
# #     local_path = path_obj.with_name(path_obj.stem + "_local" + path_obj.suffix)
    
# #     if local_path.exists():
# #         return str(local_path)
# #     elif path_obj.exists():
# #         return str(path_obj)
# #     else:
# #         return None

# def read_app_config_settings(_app_config_path):
#     """Read app configuration parameters"""
#     # _app_config_path = check_for_local_config(_app_config_path)
#     _app_config_path = Path(_app_config_path)
#     app_config = cr.read_config_file(_app_config_path)  # type: ignore
#     local_config_entry = 'app_settings'
#     output_path = app_config[local_config_entry]['output_path']
#     images_path = app_config[local_config_entry]['images_path']
#     # log_file_path = app_config[local_config_entry]['log_file_path']
#     # password_method = app_config[local_config_entry]['password_method']
#     # password_access_key = app_config[local_config_entry]['password_access_key']
#     # password_secret_key = app_config[local_config_entry]['password_secret_key']
#     # password_endpoint_url = app_config[local_config_entry]['password_endpoint_url']
#     # password_region_name = app_config[local_config_entry]['password_region_name']
#     # password_password_path = app_config[local_config_entry]['password_password_path']
#     # read_chunk_size = int(app_config[local_config_entry]['read_chunk_size'])
#     # archive_flag = app_config[local_config_entry]['archive_flag'].lower() in ['t', 'true', 'y', 'yes', '1', 'on', 'archive']
#     # logging_flag = app_config[local_config_entry]['logging_flag'].lower() in ['t', 'true', 'y', 'yes', '1', 'on', 'log']
#     # log_archive_expire_days = int(app_config[local_config_entry]['log_archive_expire_days']),
#     original_language = app_config[local_config_entry]['original_language']
#     adult_content_flag = app_config[local_config_entry]['adult_content_flag'].lower()

#     return (
#         output_path, 
#         images_path, 
#         # log_file_path, 
#         # password_method, 
#         # password_access_key, 
#         # password_secret_key, 
#         # password_endpoint_url, 
#         # password_region_name, 
#         # password_password_path, 
#         # read_chunk_size, 
#         # archive_flag, 
#         # logging_flag, 
#         # log_archive_expire_days, 
#         original_language, 
#         adult_content_flag
#         )

# def read_connection_config_settings(_connection_config_path, config_entry):
#     """Read database connection configuration"""
#     # _connection_config_path = check_for_local_config(_connection_config_path)
#     if config_entry == 'target_connection':
#         db_config = cr.read_config_file(_connection_config_path)
#         connect_type = db_config[config_entry]['connect_type']
#         server_address = db_config[config_entry]['server_address']
#         server_port = db_config[config_entry]['server_port']
#         server_name = db_config[config_entry]['server_name']
#         # schema = db_config[config_entry]['schema']
#         password_method = db_config[config_entry]['password_method']
#         password_access_key = db_config[config_entry]['password_access_key']
#         password_secret_key = db_config[config_entry]['password_secret_key']
#         password_endpoint_url = db_config[config_entry]['password_endpoint_url']
#         password_region_name = db_config[config_entry]['password_region_name']
#         password_password_path = db_config[config_entry]['password_password_path']
#         user_name = db_config[config_entry]['user_name']
#         secret_key = db_config[config_entry]['secret_key']

#         return (
#             connect_type, 
#             server_address, 
#             server_port, 
#             server_name, 
#             # schema, 
#             password_method, 
#             password_access_key, 
#             password_secret_key, 
#             password_endpoint_url, 
#             password_region_name, 
#             password_password_path, 
#             user_name, 
#             secret_key
#         )
#     elif config_entry == 'tmdb_api_connection':
#         db_config = cr.read_config_file(_connection_config_path)
#         password_method = db_config[config_entry]['password_method']
#         password_access_key = db_config[config_entry]['password_access_key']
#         password_secret_key = db_config[config_entry]['password_secret_key']
#         password_endpoint_url = db_config[config_entry]['password_endpoint_url']
#         password_region_name = db_config[config_entry]['password_region_name']
#         password_password_path = db_config[config_entry]['password_password_path']
#         user_name = db_config[config_entry]['user_name']
#         secret_key = db_config[config_entry]['secret_key']

#         return (
#             password_method, 
#             password_access_key, 
#             password_secret_key, 
#             password_endpoint_url, 
#             password_region_name, 
#             password_password_path, 
#             user_name, 
#             secret_key
#         )
#     else:
#         return None

# def read_output_file_flags(_output_files_config_path):
#     """Read output files configuration"""
#     db_config = cr.read_config_file(_output_files_config_path)
#     config_entry = 'output_files'
#     titles = db_config[config_entry].get('titles', True)
#     title_genres = db_config[config_entry].get('title_genres', True)
#     genres = db_config[config_entry].get('genres', True)
#     title_spoken_languages = db_config[config_entry].get('title_spoken_languages', True)
#     spoken_languages = db_config[config_entry].get('spoken_languages', True)
#     title_production_countries = db_config[config_entry].get('title_production_countries', True)
#     production_countries = db_config[config_entry].get('production_countries', True)
#     title_production_companies = db_config[config_entry].get('title_production_companies', True)
#     production_companies = db_config[config_entry].get('production_companies', True)
#     title_collections = db_config[config_entry].get('title_collections', True)
#     collections = db_config[config_entry].get('collections', True)
#     title_keywords = db_config[config_entry].get('title_keywords', True)
#     keywords = db_config[config_entry].get('keywords', True)
#     persons = db_config[config_entry].get('persons', True)
#     person_aka = db_config[config_entry].get('person_aka', True)
#     title_cast = db_config[config_entry].get('title_cast', True)
#     title_images = db_config[config_entry].get('title_images', True)
#     title_removed = db_config[config_entry].get('title_removed', True)
#     person_removed = db_config[config_entry].get('person_removed', True)

#     return (
#         titles, 
#         title_genres, 
#         genres, 
#         title_spoken_languages, 
#         spoken_languages, 
#         title_production_countries, 
#         production_countries, 
#         title_production_companies, 
#         production_companies, 
#         title_collections, 
#         collections, 
#         title_keywords, 
#         keywords, 
#         persons, 
#         person_aka, 
#         title_cast, 
#         title_images, 
#         title_removed, 
#         person_removed
#     )

# # def read_sql_queries(_sql_queries_config_path):
# #     """Read sql query configuration"""
# #     # _sql_queries_config_path = check_for_local_config(_sql_queries_config_path)
# #     db_config = cr.read_config_file(_sql_queries_config_path)
# #     config_entry = 'sql_statements'
# #     loaded_titles_sql = db_config[config_entry]['loaded_titles_sql']
# #     loaded_title_cast_sql = db_config[config_entry]['loaded_title_cast_sql']
# #     loaded_persons_sql = db_config[config_entry]['loaded_persons_sql']
# #     loaded_title_images_sql = db_config[config_entry]['loaded_title_images_sql']
# #     favorite_persons_sql = db_config[config_entry]['favorite_persons_sql']
# #     search_terms_sql = db_config[config_entry]['search_terms_sql']
# #     titles_missing_cast_sql = db_config[config_entry]['titles_missing_cast_sql']
# #     titles_missing_keywords_sql = db_config[config_entry]['titles_missing_keywords_sql']
# #     persons_missing_sql = db_config[config_entry]['persons_missing_sql']

# #     return (
# #         loaded_titles_sql, 
# #         loaded_title_cast_sql, 
# #         loaded_persons_sql, 
# #         loaded_title_images_sql, 
# #         favorite_persons_sql, 
# #         search_terms_sql, 
# #         titles_missing_cast_sql, 
# #         titles_missing_keywords_sql, 
# #         persons_missing_sql
# #     )
