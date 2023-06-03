#%%
import pandas as pd
from sqlalchemy import text
from nexus_utils import database_utils


class LocalDB:
    def __init__(
            self, 
            engine, 
            adult_content_flag, 
            loaded_titles_sql=None, 
            loaded_title_cast_sql=None, 
            loaded_persons_sql=None, 
            loaded_title_images_sql=None, 
            favorite_persons_sql=None, 
            search_terms_sql=None, 
            titles_missing_cast_sql=None, 
            titles_missing_keywords_sql=None, 
            persons_missing_sql=None
        ):

        self.engine = engine
        functioning_engine = False
        self._engine_status = 'Unknown status'
        if self.engine is None:
            self._engine_status = 'DB Engine could not be built'
        else:
            try:
                result = database_utils.check_engine_read(engine)
                if result == 'Success':
                    functioning_engine = True
                    self._engine_status = 'DB Engine Functional'
                else:
                    self._engine_status = f'DB Engine failed:  {result}'
            except Exception as e:
                self._engine_status = 'DB Engine failed to read from database'
            
        self.adult_content_flag = adult_content_flag

        self.error_tmdb_id_list = []
        self.error_person_id_list = []

        if loaded_titles_sql:
            self.loaded_titles_sql = loaded_titles_sql

        if loaded_titles_sql:
            self.loaded_title_cast_sql = loaded_title_cast_sql

        if loaded_persons_sql:
            self.loaded_persons_sql = loaded_persons_sql

        if loaded_title_images_sql:
            self.loaded_title_images_sql = loaded_title_images_sql

        if favorite_persons_sql:
            self.favorite_persons_sql = favorite_persons_sql

        if search_terms_sql:
            self.search_terms_sql = search_terms_sql

        if titles_missing_cast_sql:
            self.titles_missing_cast_sql = titles_missing_cast_sql

        if titles_missing_keywords_sql:
            self.titles_missing_keywords_sql = titles_missing_keywords_sql

        if persons_missing_sql:
            self.persons_missing_sql = persons_missing_sql
        
        self._loaded_titles_checked_flag = False
        self._loaded_titles = []
        self._loaded_title_cast_checked_flag = False
        self._loaded_title_cast = []
        self._loaded_persons_checked_flag = False
        self._loaded_persons = []
        self._loaded_title_images_checked_flag = False
        self._loaded_title_images = []
        self._favorite_persons_checked_flag = False
        self._favorite_persons = []
        self._search_terms_checked_flag = False
        self._search_terms = []
        self._titles_missing_cast_checked_flag = False
        self._titles_missing_cast = []
        self._titles_missing_keywords_checked_flag = False
        self._titles_missing_keywords = []
        self._persons_missing_checked_flag = False
        self._persons_missing = []

        if not functioning_engine:
            print(self._engine_status)
            self._loaded_titles_checked_flag = True
            self._loaded_title_cast_checked_flag = True
            self._loaded_persons_checked_flag = True
            self._loaded_title_images_checked_flag = True
            self._favorite_persons_checked_flag = True
            self._search_terms_checked_flag = True
            self._titles_missing_cast_checked_flag = True
            self._titles_missing_keywords_checked_flag = True
            self._persons_missing_checked_flag = True

    @property
    def engine_status(self):
        """Engine status message"""
        return self._engine_status

    @property
    def loaded_titles(self):#, select_query=None):
        """Retrieve all TMDB IDs already loaded"""

        if not self._loaded_titles_checked_flag:
            if self.loaded_titles_sql:
                select_query = self.loaded_titles_sql
                    
                try:
                    with self.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                        # print('loaded_titles: querying database')
                        results = conn.execute(text(select_query))
                        df = pd.DataFrame(results)

                    self._loaded_titles_checked_flag = True
                    
                    if not df.empty:
                        df.columns = ['tmdb_id']
                        self._loaded_titles = df['tmdb_id'].tolist()
                    else:
                        self._loaded_titles = []
                except Exception as e:
                    self._loaded_titles_checked_flag = True
                    self._loaded_titles = []
            else:
                self._loaded_titles = []

        return self._loaded_titles

    @property
    def loaded_title_cast(self):#, select_query=None):
        """Retrieve all TMDB IDs already loaded in Title Cast"""

        if not self._loaded_title_cast_checked_flag:
            if self.loaded_title_cast_sql:
                select_query = self.loaded_title_cast_sql
            
                try:
                    with self.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                        # print('loaded_title_cast: querying database')
                        results = conn.execute(text(select_query))
                        df = pd.DataFrame(results)

                    self._loaded_title_cast_checked_flag = True

                    if not df.empty:
                        df.columns = ['tmdb_id']
                        self._loaded_title_cast = df['tmdb_id'].tolist()
                    else:
                        self._loaded_title_cast = []
                except Exception as e:
                    self._loaded_title_cast_checked_flag = True
                    self._loaded_title_cast = []
            else:
                self._loaded_title_cast = []

        return self._loaded_title_cast

    @property
    def loaded_persons(self):#, select_query=None):
        """Retrieve all Person IDs already loaded"""

        if not self._loaded_persons_checked_flag:
            if self.loaded_persons_sql:
                select_query = self.loaded_persons_sql
                
                try:
                    with self.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                        # print('loaded_persons: querying database')
                        results = conn.execute(text(select_query))
                        df = pd.DataFrame(results)

                    self._loaded_persons_checked_flag = True
                    
                    if not df.empty:
                        df.columns = ['person_id']
                        self._loaded_persons = df['person_id'].tolist()
                    else:
                        self._loaded_persons = []
                except Exception as e:
                    self._loaded_persons_checked_flag = True
                    self._loaded_persons = []
            else:
                self._loaded_persons = []

        return self._loaded_persons

    @property
    def loaded_title_images(self):#, select_query=None):
        """Retrieve all TMDB IDs already loaded"""

        if not self._loaded_title_images_checked_flag:
            if self.loaded_title_images_sql:
                select_query = self.loaded_title_images_sql
                
                try:
                    with self.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                        # print('loaded_title_images: querying database')
                        results = conn.execute(text(select_query))
                        df = pd.DataFrame(results)

                    self._loaded_title_images_checked_flag = True

                    if not df.empty:
                        df.columns = ['tmdb_id']
                        self._loaded_title_images = df['tmdb_id'].tolist()
                    else:
                        self._loaded_title_images = []
                except Exception as e:
                    self._loaded_title_images_checked_flag = True
                    self._loaded_title_images = []
            else:
                self._loaded_title_images = []

        return self._loaded_title_images

    @property
    def favorite_persons(self):#, select_query=None):
        """Retrieve favorite persons"""

        if not self._favorite_persons_checked_flag:
            if self.favorite_persons_sql:
                select_query = self.favorite_persons_sql
                
                try:
                    with self.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                        # print('favorite_persons: querying database')
                        results = conn.execute(text(select_query))
                        df = pd.DataFrame(results)

                    self._favorite_persons_checked_flag = True

                    if not df.empty:
                        df.columns = ['person_id', 'adult_flag']
                        if self.adult_content_flag == 'exclude':
                            self._favorite_persons = df[df['adult_flag'] == 'F']['person_id'].tolist()
                        elif self.adult_content_flag == 'only':
                            self._favorite_persons = df[df['adult_flag'] == 'T']['person_id'].tolist()
                        else:
                            self._favorite_persons = df['person_id'].tolist()
                    else:
                        self._favorite_persons = []
                except Exception as e:
                    self._favorite_persons_checked_flag = True
                    self._favorite_persons = []
            else:
                self._favorite_persons = []

        return self._favorite_persons

    @property
    def search_terms(self):#, select_query=None):
        """Retrieve search terms"""

        if not self._search_terms_checked_flag:
            if self.search_terms_sql:
                select_query = self.search_terms_sql
                
                try:
                    with self.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                        # print('search_terms: querying database')
                        results = conn.execute(text(select_query))
                        df = pd.DataFrame(results)
                        df.columns = ['search_term']

                    self._search_terms_checked_flag = True

                    if not df.empty:
                        df.columns = ['search_term']
                        self._search_terms = df['search_term'].tolist()
                    else:
                        self._search_terms = []
                except Exception as e:
                    self._search_terms_checked_flag = True
                    self._search_terms = []
            else:
                self._search_terms = []

        return self._search_terms

    @property
    def titles_missing_cast(self):#, select_query=None):
        """Retrieve titles missing cast"""

        if not self._titles_missing_cast_checked_flag:
            if self.titles_missing_cast_sql:
                select_query = self.titles_missing_cast_sql
                
                try:
                    with self.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                        # print('titles_missing_cast: querying database')
                        results = conn.execute(text(select_query))
                        df = pd.DataFrame(results)

                    self._titles_missing_cast_checked_flag = True

                    if not df.empty:
                        df.columns = ['tmdb_id', 'adult_flag']
                        if self.adult_content_flag == 'exclude':
                            self._titles_missing_cast = df[df['adult_flag'] == 'F']['tmdb_id'].tolist()
                        elif self.adult_content_flag == 'only':
                            self._titles_missing_cast = df[df['adult_flag'] == 'T']['tmdb_id'].tolist()
                        else:
                            self._titles_missing_cast = df['tmdb_id'].tolist()
                    else:
                        self._titles_missing_cast = []
                except Exception as e:
                    self._titles_missing_cast_checked_flag = True
                    self._titles_missing_cast = []
            else:
                self._titles_missing_cast = []

        return self._titles_missing_cast

    @property
    def titles_missing_keywords(self):#, select_query=None):
        """Retrieve titles missing keywords"""

        if not self._titles_missing_keywords_checked_flag:
            if self.titles_missing_keywords_sql:
                select_query = self.titles_missing_keywords_sql
                
                try:
                    with self.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                        # print('titles_missing_keywords: querying database')
                        results = conn.execute(text(select_query))
                        df = pd.DataFrame(results)

                    self._titles_missing_keywords_checked_flag = True

                    if not df.empty:
                        df.columns = ['tmdb_id', 'adult_flag']
                        if self.adult_content_flag == 'exclude':
                            self._titles_missing_keywords = df[df['adult_flag'] == 'F']['tmdb_id'].tolist()
                        elif self.adult_content_flag == 'only':
                            self._titles_missing_keywords = df[df['adult_flag'] == 'T']['tmdb_id'].tolist()
                        else:
                            self._titles_missing_keywords = df['tmdb_id'].tolist()
                    else:
                        self._titles_missing_keywords = []
                except Exception as e:
                    self._titles_missing_keywords_checked_flag = True
                    self._titles_missing_keywords = []
            else:
                self._titles_missing_keywords = []

        return self._titles_missing_keywords

    @property
    def persons_missing(self):#, select_query=None):
        """Retrieve missing persons"""

        if not self._persons_missing_checked_flag:
            if self.persons_missing_sql:
                select_query = self.persons_missing_sql
                
                try:
                    with self.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                        # print('persons_missing: querying database')
                        results = conn.execute(text(select_query))
                        df = pd.DataFrame(results)

                    self._persons_missing_checked_flag = True

                    if not df.empty:
                        df.columns = ['person_id', 'adult_flag']
                        if self.adult_content_flag == 'exclude':
                            self._persons_missing = df[df['adult_flag'] == 'F']['person_id'].tolist()
                        elif self.adult_content_flag == 'only':
                            self._persons_missing = df[df['adult_flag'] == 'T']['person_id'].tolist()
                        else:
                            self._persons_missing = df['person_id'].tolist()
                    else:
                        self._persons_missing = []
                except Exception as e:
                    self._persons_missing_checked_flag = True
                    self._persons_missing = []
            else:
                self._persons_missing = []

        return self._persons_missing
