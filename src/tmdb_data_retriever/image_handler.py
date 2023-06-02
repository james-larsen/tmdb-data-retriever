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

    def get_image_data(self, tmdb_id_list, ids_to_skip=[]):
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
                if results['backdrops']:
                    for result in results['backdrops']:
                        result['tmdb_id'] = results['id']
                        result['image_type'] = 'backdrop'
                        all_results.append(result)
                        # print(result)
                
                if results['logos']:
                    for result in results['logos']:
                        result['tmdb_id'] = results['id']
                        result['image_type'] = 'logo'
                        all_results.append(result)
                        # print(result)
                
                if results['posters']:
                    for result in results['posters']:
                        result['tmdb_id'] = results['id']
                        result['image_type'] = 'poster'
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
