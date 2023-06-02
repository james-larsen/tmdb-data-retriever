from pathlib import Path
import glob
import datetime
import dateparser

def write_data_to_file(df, output_path, base_filename, suffix=None):
    
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    if suffix:
        filename = base_filename + '_' + suffix
    else:
        filename = base_filename
    
    output_file = Path(output_path, filename + '.csv')

    df.to_csv(output_file, sep='\t', index=False, header=True, encoding='utf-8', quoting=1)

def cleanse_value(value):
    if isinstance(value, str):
        value = value.replace('\r\n', ' | ')
        value = value.replace('\r', ' | ')
        value = value.replace('\n', ' | ')
        value = value.replace('\t', ' ')
        value = value.replace('  ', ' ')
    elif isinstance(value, dict):
        value = {key: cleanse_value(val) if isinstance(val, str) else val for key, val in value.items()}
    return value

def format_dates(start_date=None, end_date=None):
    if start_date is not None:
        start_date = dateparser.parse(start_date).date()
    else:
        start_date = dateparser.parse('yesterday').date()

    if end_date is not None:
        end_date = dateparser.parse(end_date).date()
    else:
        end_date = dateparser.parse('yesterday').date()

    if isinstance(start_date, str):
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    if isinstance(end_date, str):
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

    delta = end_date - start_date

    date_list = [start_date + datetime.timedelta(days=i) for i in range(delta.days + 1)]

    return date_list

def read_sql_queries(sql_queries_path):
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
        titles_missing_cast_sql, 
        titles_missing_keywords_sql, 
        persons_missing_sql
    )
