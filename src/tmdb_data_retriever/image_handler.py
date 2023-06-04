#%%
import os
import requests
import pandas as pd
# from sqlalchemy import text
import time
import utils.misc_utils as misc


class ImageData:
    def __init__(
            self, 
            api_key, 
            local_db, 
            output_path, 
            image_path, 
            output_title_images_flag=True):
        
        self.api_key = api_key
        self.local_db = local_db
        self.output_path = output_path
        self.image_path = image_path
        self.output_title_images_flag = output_title_images_flag

    def get_image_data(self, tmdb_id_list, ids_to_skip=[], backdrop_flag=True, poster_flag=True, logo_flag=True):
        """Retrieve film data given a list of TMDB IDs"""
        # global api_key
        all_results = []

        # remove duplicates
        tmdb_id_list = list(set(tmdb_id_list))
        list_len = len(tmdb_id_list)

        tmdb_id_list = [item for item in tmdb_id_list if item not in ids_to_skip]

        params = {
            'api_key': self.api_key
        }

        print('Retrieving title images list')

        for i, tmdb_id in enumerate(tmdb_id_list):
            url = f'https://api.themoviedb.org/3/movie/{tmdb_id}/images'

            response = requests.get(url, params=params)
            results = response.json()
            if 'success' not in results:
                if results['backdrops'] and backdrop_flag:
                    for result in results['backdrops']:
                        result['tmdb_id'] = results['id']
                        result['image_type'] = 'backdrop'
                        all_results.append(result)
                        # print(result)
                
                if results['posters'] and poster_flag:
                    for result in results['posters']:
                        result['tmdb_id'] = results['id']
                        result['image_type'] = 'poster'
                        all_results.append(result)
                        # print(result)
                
                if results['logos'] and logo_flag:
                    for result in results['logos']:
                        result['tmdb_id'] = results['id']
                        result['image_type'] = 'logo'
                        all_results.append(result)
                        # print(result)
            
                if (i + 1) % 40 == 0:
                    print(f'Titles processed: {i + 1} of {list_len}')
                    time.sleep(2)
            else:
                self.local_db.error_tmdb_id_list.append(tmdb_id)
        
        if all_results:
            df_images = pd.DataFrame(all_results)
        else:
            df_images = pd.DataFrame()
        
        return df_images
    
    def process_images(self, df_images, suffix):

        self.download_images(df_images)

        output_path = self.output_path

        if self.output_title_images_flag:
            misc.write_data_to_file(df_images, output_path + os.sep + 'tmdb_title_image', 'tmdb_title_image', suffix)

    def download_images(self, df_images):

        tmdb_ids = df_images['tmdb_id'].unique()

        images_processed = 0

        print('Retrieving title imagess')

        for tmdb_id in tmdb_ids:
        
            # filtered_df_images = df_images[df_images['tmdb_id'] == your_tmdb_id].copy()
            
            local_image_path = os.path.join(self.image_path, 'tmdb_id_' + str(tmdb_id))
            
            if not os.path.exists(local_image_path):
                os.makedirs(local_image_path)

            df_images_tmdb_id = df_images[df_images['tmdb_id'] == tmdb_id].copy()
            available_image_types = df_images_tmdb_id['image_type'].unique()

            for image_type in available_image_types:
                local_image_path_subfolder = os.path.join(local_image_path, image_type)
                
                if not os.path.exists(local_image_path_subfolder):
                    os.makedirs(local_image_path_subfolder)

                image_num = 1
                for index, image in df_images.iterrows():
                    if image['tmdb_id'] == tmdb_id and image['image_type'] == image_type:
                        tmdb_id = image['tmdb_id']
                        file_path = image['file_path']
                        local_file_name = str(tmdb_id) + '_' + image_type + '_' + str(image_num).zfill(2) + '_' + file_path[1:]
                        local_file_path = os.path.join(local_image_path_subfolder, local_file_name)

                        url = f'https://www.themoviedb.org/t/p/original{file_path}'
                        response = requests.get(url)
                        if response.status_code == 200:
                            with open(local_file_path, 'wb') as file:
                                file.write(response.content)
                            df_images.at[index, 'local_file_path'] = str(local_file_path)
                            image_num += 1
                            images_processed += 1
                        else:
                            print(f'URL: {url}\nResponse: {response.status_code}')

                        if images_processed % 10 == 0:
                            print(f'Images processed: {images_processed} of {len(df_images)}')
                            time.sleep(2)

    def create_title_image_html(self, tmdb_id_list=[], include_keywords=True, html_path=None, html_name=None):

        if not html_path:
            html_path = self.image_path
        
        if not html_name or '.html' not in html_name:
            html_name = 'index.html'

        df_title_images = self.local_db.title_images_by_favorite_persons.copy()

        if tmdb_id_list:
            df_title_images = df_title_images[df_title_images['tmdb_id'].isin(tmdb_id_list)]
        
        # Confirm images exist
        for index, row in df_title_images.iterrows():
            file_path = row['local_file_path']
            if not os.path.exists(file_path):
                df_title_images.drop(index, inplace=True)
        
        df_titles = df_title_images.loc[:, ['tmdb_id','title_name','has_backdrop','person_list','keyword_list']].copy()
        df_titles = df_titles.drop_duplicates()
        df_posters = df_title_images.loc[df_title_images['image_type'] == 'poster', ['tmdb_id', 'image_type', 'local_file_path']].copy()
        df_posters = df_posters.drop_duplicates()
        df_backdrops = df_title_images.loc[df_title_images['image_type'] == 'backdrop', ['tmdb_id', 'image_type', 'local_file_path']].copy()
        df_backdrops = df_backdrops.drop_duplicates()

        html_string = """
        <html>
            <head>
                <title>TMDB Title Images</title>
            </head>
            <body>
                <style>
                    .poster-image {
                        max-width: 300px;
                        max-height: 300px;
                        border: 1px solid black;
                    }
                    .backdrop-image {
                        max-width: 1200px;
                        border: 1px solid black;
                    }
                </style>
                <h1>TMDB Title Images</h1>
                <br><br>
        """

        for index, row in df_titles.iterrows():
            tmdb_id = row['tmdb_id']
            title_name = row['title_name']
            person_list = row['person_list']
            keyword_list = row['keyword_list']
            poster_rows = df_posters.loc[df_posters['tmdb_id'] == tmdb_id].copy().reset_index(drop=True)
            backdrop_rows = df_backdrops.loc[df_backdrops['tmdb_id'] == tmdb_id].copy().reset_index(drop=True)

            html_string += f"""
                <hr>
                <h2>{title_name} (tmdb_id: <a href="https://www.themoviedb.org/movie/{tmdb_id}" target="_blank">{tmdb_id}</a>)</h2>
                <b>Cast List: </b>
                <br>
                <b>{person_list}</b>
                <br><br>
            """

            if keyword_list and include_keywords:
                html_string += f"""
                    <b>Keyword List:</b>
                    <br>
                    <b>{keyword_list}</b>
                    <br><br>
                """

            if len(poster_rows) > 0:
                poster_path = poster_rows['local_file_path'].iloc[0].replace(' ', '%20').replace('\\', '/')
                html_string += f'<img src="file:///{poster_path}" alt="{tmdb_id} Poster"  class="poster-image">'

            if len(backdrop_rows) > 0:
                for backdrop_index, backdrop_row in enumerate(backdrop_rows.itertuples(), start=1):
                    backdrop_path = backdrop_row.local_file_path.replace(' ', '%20').replace('\\', '/')
                    html_string += f"""
                        <br><br>
                        <img src="file:///{backdrop_path}" alt="{tmdb_id} Backdrop {backdrop_index}"  class="backdrop-image">
                    """

        html_string += """
        </body>
        </html>
        """

        with open(html_path + '/' + html_name, "w") as file:
            file.write(html_string)
