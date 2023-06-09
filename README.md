# TMDB Data Retriever<!-- omit in toc -->

The purpose of this application is to connect to The Movie Database (TMDB) API to extract various movie and cast details as flat files, intended to be loaded to a separate database.  It also has the ability to connect to the target database to extract certain datasets, particularly which titles and cast members have already been extracted.

You will need to acquire a free TMDB API key to use this application.  Details can be found at the location below:  
[https://developer.themoviedb.org/docs/authentication-application](https://developer.themoviedb.org/docs/authentication-application)

## Table of Contents <!-- omit in toc -->
- [Requirements](#requirements)
- [Installation](#installation)
- [Passwords](#passwords)
- [App Configuration](#app-configuration)
  - [SQL Scripts](#sql-scripts)
- [Usage](#usage)
  - [Command-Line Flags](#command-line-flags)
  - [Function Arguments](#function-arguments)
  - [Example Usage](#example-usage)
- [Docker Deployment with S3](#docker-deployment-with-s3)
  - [S3 Folder Structure](#s3-folder-structure)
  - [Deploying the Container](#deploying-the-container)
- [About the Author](#about-the-author)

---

## Requirements

python > 3.8

pandas >=1.5.3, <2.0.0

sqlalchemy >=2.0.4, <3.0.0

flask >=2.2.5, <3.0.0

waitress >=2.1.2 ,<3.0.0

psycopg2-binary >=2.9.5, <3.0.0

configparser >=5.3.0, <6.0.0

dateparser >=1.1.8, <2.0.0

keyring >=23.13.1, <24.0.0

nexus-utilities >=0.6.0, <1.0.0 # My custom utilities package

## Installation

```python
pip3 install pandas >=1.5.3,<2.0.0
pip3 install sqlalchemy >=2.0.4,<3.0.0
pip3 install flask >=2.2.5,<3.0.0
pip3 install waitress >=2.1.2,<3.0.0
pip3 install psycopg2-binary >=2.9.5,<3.0.0
pip3 install configparser >=5.3.0,<6.0.0
pip3 install dateparser >=1.1.8,<2.0.0
pip3 install keyring >=23.13.1,<24.0.0
pip3 install nexus-utilities >=0.6.0,<1.0.0 # My custom utilities package

OR

# From the package root directory
pip3 install -r requirements.txt
```

From the package root:

```python
pip3 install .
```

**Note that this is needed so relative imports will function**

## Passwords

The modules for retrieving secured information are located in the nexus-utilities package. The desired method should be specified in the app_config.ini file. All methods accept two required strings of 'password_method' and 'password_key' and a number of optional arguments, and return a string of 'secret_value'.  See the documentation for nexus-utilities at [https://github.com/james-larsen/nexus-utilities](https://github.com/james-larsen/nexus-utilities) for more details

If you do decide to use the keyring library, you will need to add entries using the "user_name" and "secret_key" from the connections_config.ini file for both the TMDB API key and the target DB password (optional):

```python
keyring.set_password("{user_name}", "{secret_key}", "my_api_key")
keyring.set_password("{user_name}", "{secret_key}", "my_db_password")
```

Alternatively, if the below OS Environment variables are available, they will be used instead:
 - NEXUS_TDR_TARGET_DB_PASSWORD
 - NEXUS_TMDB_API_KEY

## App Configuration

The application is controlled by a number of configuration files, read using the ConfigParser library:

**./config/app_config.ini**

Controls the general behavior of the application (example values provided)

``` python
[app_settings]
# Root location to place extracted flat files
load_file_path = 
# Root location to place extracted images
images_path = 
# Restrict results to a specific language (2-digit iso code, Eg. "en")
original_language = en
# Whether to include adult content in results.  Accepts "Include", "Exclude" and "Only"
adult_content_flag = Exclude
```

**./config/connections_config.ini**

Holds connection details for the TMDB API and target database.  Builds a SQLAlchemy connection string with the following pattern:

'{connect_type}://{user_name}:{password}@{server_address}:{server_port}/{server_name}'

``` python
[target_connection]
# SQLAlchemy connection type
connect_type = postgresql+psycopg2
# Environment for connection (dev / qa / prod).  Informational only
environment = dev
server_address = localhost
server_port = 5432
server_name = 
# Method for retrieving secrets.  Accepts "keyring", "secretsmanager" or "ssm"
password_method = ssm
# Access key for secrets retrieval method
password_access_key = 
# Secret key for secrets retrieval method
password_secret_key = 
# Enpoint URL for secrets retrieval method
password_endpoint_url = https://ssm.us-west-1.amazonaws.com
# Region name for secrets retrieval method
password_region_name = us-west-1
# Path for secrets retrieval method
password_password_path = my_program/passwords/dev
user_name = 
# Reference value for retrieving the correct password using keyring library
secret_key = 

[tmdb_api_connection]
# Method for retrieving secrets.  Accepts "keyring", "secretsmanager" or "ssm"
password_method = ssm
# Access key for secrets retrieval method
password_access_key = 
# Secret key for secrets retrieval method
password_secret_key = 
# Enpoint URL for secrets retrieval method
password_endpoint_url = https://ssm.us-west-1.amazonaws.com
# Region name for secrets retrieval method
password_region_name = us-west-1
# Path for secrets retrieval method
password_password_path = my_program/passwords/dev
user_name = 
secret_key = 
```

**./config/output_files_config.ini**

Controls which data files are generated by the application

``` python
[output_files]
# Title details
titles = True
# Title / genre combinations
title_genres = True
# Genre details
genres = True
# Title / spoken language combinations
title_spoken_languages = True
# Spoken language details
spoken_languages = True
# Title / production country combinations
title_production_countries = True
# Production country details
production_countries = True
# Title / production company combinations
title_production_companies = True
# Production company details
production_companies = True
# Title / collection combinations
title_collections = True
# Collection details
collections = True
# Title / keyword combinations
title_keywords = True
# Keyword details
keywords = True
# Person details
persons = True
# Person AKA details
person_aka = True
# Title / person combinations
title_cast = True
# Title / image combinations
title_images = True
# Titles no longer found
title_removed = True
# Persons no longer found
person_removed = True
```

---
### SQL Scripts

A number of sql files are located in **./config/sql_queries/**.  These are used for connecting to the target DB and retrieving certain information, such as already loaded titles, or title cast without person details.  This is completely optional, and if proper details for the target DB or the sql files are not provided, the functionality will not be used.  
***Note:*** The field names don't matter, but the number, order, and format of the fields do.  If your database has no concept of "adult content", hardcode "F" where necessary

**./config/sql_queries/favorite_persons_sql.sql**

List of favorite persons

**Expected Select Output:**
* person_id (int)
* adult_flag (str: "T" or "F")

---

**./config/sql_queries/loaded_persons_sql.sql**

List of already loaded persons

**Expected Select Output:**
* person_id (int)
* adult_flag (str: "T" or "F")

---

**./config/sql_queries/loaded_title_cast_sql.sql**

List of already loaded titles with cast linkages

**Expected Select Output:**
* tmdb_id (int)

---

**./config/sql_queries/loaded_title_images_sql.sql**

List of already loaded titles with downloaded images

**Expected Select Output:**
* tmdb_id (int)

---

**./config/sql_queries/loaded_titles_sql.sql**

List of already loaded titles

**Expected Select Output:**
* tmdb_id (int)
* adult_flag (str: "T" or "F")

---

**./config/sql_queries/persons_missing_sql.sql**

List of persons that appear in title cast, but do not have person details.  I suggest using a "min" function on the "adult_flag" to include persons who appear in both adult and non-adult titles (unless you intend to use adult "Only", in which case use a "max" function)

**Expected Select Output:**
* person_id (int)
* adult_flag (str: "T" or "F")

---

**./config/sql_queries/search_terms_sql.sql**

List of string search terms to attempt to find titles with.  Useful if you have another table containing title names you'd like to attempt to pull TMDB title data for

**Expected Select Output:**
* search_term (str)

---

**./config/sql_queries/titles_missing_cast_sql.sql**

List of loaded titles without any cast linkages

**Expected Select Output:**
* tmdb_id (int)
* adult_flag (str: "T" or "F")

---

**./config/sql_queries/titles_missing_keywords_sql.sql**

List of loaded titles without any keyword linkages

**Expected Select Output:**
* tmdb_id (int)
* adult_flag (str: "T" or "F")

## Usage

Configure the following files (details above):

* ./config/app_config.ini
* ./config/connections_config.ini
* ./config/output_files_config.ini
* ./config/sql_queries/*.sql

The application uses a number of function arguments with optional flags.  The flags will be described first, then the arguments along with which flags they are compatible with.  Note that all arguments and flags have --long and -short representations.

### Command-Line Flags

* --original_language (-lang): Primary language spoken in the title.  Accepts 2 digit, lowercase iso standard codes
* --min_runtime (-rt): Minimum title runtime.  Use with caution, as many titles erroneously use 1 minute as a placeholder
* --adult_content_flag (-adult): Whether to include adult content in results.  Accepts "include" ("i"), "exclude" ("e") or "only" ("o")
* --skip_loaded_titles (-skip): Add the "-skip" flag to avoid pulling titles already pulled previously.  Relies on the target DB connection capabilities discussed above
* --search_terms (-search): Accepts multiple string arguments of keywords to search by
* --person_ids (-pid): Accepts multiple person_id integer values
* --row_limit (-rl):  Row limit to apply to requests.  Note that this won't always exactly reflect in the number of results that are returned, depending on the kind of data being retrieved.  Used primarily to limit number of requests sent to the TMDB API
* --time_window (-tw): Accepts "day" ("d") or "week" ("w")

### Function Arguments

* display_missing_counts (dmc): Display the current number of missing cast, keywords and persons
* get_movies_updated_yesterday (gmuy): Retrieves movies changed yesterday
    * --original_language
    * --min_runtime
    * --adult_content_flag
* get_movies_by_favorite_actor (gmbfa): Retrieve titles with specified persons as cast members
    * --person_id_list - If omitted, will utilize the results of the query "favorite_persons_sql.sql"
    * --adult_content_flag
    * --skip_loaded_titles
    * --row_limit
* get_movies_by_search_terms (gmbst): Retrieve titles matching provided search terms
    * --search_terms - If omitted, will utilize the results of the query "search_terms_sql.sql"
    * --original_language
    * --adult_content_flag
    * --skip_loaded_titles
    * --row_limit
* get_trending_movies (gtm): Retrieve trending titles over the last day or week.  Returns a maximum of 20,000 titles
    * --time_window
    * --original_language
    * --skip_loaded_titles
    * --row_limit
* get_missing_title_keywords (gmtk): Retrieve keywords linkages for titles without any
    * --adult_content_flag
    * --row_limit
* get_missing_persons (gmp): Retrieve missing persons referenced in the title cast data
    * --adult_content_flag
    * --row_limit
* get_missing_title_cast (gmtc): Retrieve cast linkages for titles without any
    * --adult_content_flag
    * --row_limit
* get_all_movies (gam): Downloads the daily full movie list with a subset of fields
* get_all_persons (gap): Downloads the daily full person list with a subset of fields
* reconcile_movies_against_full_list (rmafl): Checks the currently loaded list of titles against today's full list to identify removed titles.  Note that the daily lists do not contain adult content, so if your data does, ensure "loaded_titles_sql" is configured properly to not trigger false positives as removed titles
* reconcile_persons_against_full_list (rpafl): Checks the currently loaded list of persons against today's full list to identify removed persons.  Note that the daily lists do not contain adult content, so if your data does, ensure "loaded_persons_sql" is configured properly to not trigger false positives as removed persons

### Example Usage
```python
python3 ../src/main.py dmc

python3 ../src/main.py get_movies_updated_yesterday -lang "en" -adult "exclude"

python3 ../src/main.py get_movies_by_favorite_actor -pid 1158 3223 -skip

python3 ../src/main.py gmbst -search "fast 9" "a christmas story" -lang "en" -adult "exclude" -skip

python3 ../src/main.py get_trending_movies -tw "week" -lang "jp" -rl 6000 --skip_loaded_titles

python3 ../src/main.py get_missing_title_keywords --adult_content_flag "include"

python3 ../src/main.py get_missing_persons -adult "o"

python3 ../src/main.py get_missing_title_cast -adult "e" --row_limit 500
```

## Docker Deployment with S3

A Docker image has been created based on the "tiangolo/uwsgi-nginx" Linux image, with all necessary files and libraries deployed.  It can be found at "jameslarsen42/nexus_tmdb_data_retriever".  Alternatively, a Dockerfile has been included in this package if you wish to build it yourself.

### S3 Folder Structure

The Docker Container uses s3sf fuse to mount an S3 bucket to specific locations used by the application.  An S3 bucket should be created with a certain sub-folder structure.  A template can be found at **./templates/S3 Folder Structure/**.  Note that the "app_config.ini" in this folder has already been optimized to point to the correct locations for Uploads, but other settings should be customized before uploading to S3.  Similarly make sure to customize "connections_config.ini" for your TMDB API key and target database.  The file "docker.env" is not used within the application, but can be useful when launching the Docker Container, if you prefer to use environment variables rather than storing sensitive information in the .ini files.

The below environment variables are required to be defined when launching the container in order for the S3 mounts to work:
*  ***AWS_ACCESS_KEY_ID***
*  ***AWS_SECRET_ACCESS_KEY***
*  ***S3_SERVER_PATH***

### Deploying the Container

You can specify variables directly if you like, but the simplest method is below, after customizing your "docker.env" file.  Note that the "--cap-add SYS_ADMIN --device /dev/fuse" is necessary for the S3 mounts to work properly.

``` bash
docker run --env-file file/path/to/docker.env --cap-add SYS_ADMIN --device /dev/fuse -it nexus_tmdb_data_retriever
```

## About the Author

My name is James Larsen, and I have been working professionally as a Business Analyst, Database Architect and Data Engineer since 2007.  While I specialize in Data Modeling and SQL, I am working to improve my knowledge in different data engineering technologies, particularly Python.

[https://www.linkedin.com/in/jameslarsen42/](https://www.linkedin.com/in/jameslarsen42/)