import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests import *
import re
import time
import numpy as np
import random
import dateparser

class BooliApartments:

    def __init__(self):
        self.base_url = "https://www.booli.se"
        # self.max_sold_date = max_sold_date
        # self.min_sold_date = min_sold_date
        self.region = 'stockholms+innerstad'
        self.region_id = '143'
        # self.url_sold_apartments = self.base_url + '/slutpriser/' + self.region + '/' + self.region_id + '/?maxSoldDate=' + self.max_sold_date + '&minSoldDate=' + self.min_sold_date + '&objectType=Lägenhet'
        
        self.df_col_growth = 'Growth'
        self.df_col_address = 'Address'
        self.df_col_rooms = 'Rooms'
        self.df_col_m2 = 'm2'
        self.df_col_size_in_m2 = 'SizeInM2'
        self.df_col_extended_size_in_m2 = 'ExtendedSizeInM2'
        self.df_col_region = 'Region'
        self.df_col_initial_price = 'InitialPrice'
        self.df_col_price_per_m2 = 'PricePerM2'
        self.df_col_date_sold = 'DateSold'
        self.df_col_link = 'Link'
        
        self.df_scraped_cols = ['Growth', 'Address', 'RoomM2', 'TypeRegion', 'InitialPrice', 'PricePerM2', 'DateSold']
        
        self.df_cols = [self.df_col_growth, 
                        self.df_col_address, 
                        self.df_col_rooms, 
                        self.df_col_m2, 
                        self.df_col_region, 
                        self.df_col_initial_price, 
                        self.df_col_price_per_m2, 
                        self.df_col_date_sold, 
                        self.df_col_link]

        self.df_cols_extended = [self.df_col_growth, 
                        self.df_col_address, 
                        self.df_col_rooms, 
                        self.df_col_size_in_m2,
                        self.df_col_extended_size_in_m2, 
                        self.df_col_region, 
                        self.df_col_initial_price, 
                        self.df_col_price_per_m2, 
                        self.df_col_date_sold, 
                        self.df_col_link]

        self.df_cols_split_room_m2 = [self.df_col_rooms, 
                        self.df_col_m2]

        self.df_cols_split_type_region = ['Type', 
                        self.df_col_region]

        self.df_cols_split_m2 = [self.df_col_size_in_m2, 
                        self.df_col_extended_size_in_m2]

        self.df_cols_numeric = [self.df_col_growth, 
                        self.df_col_initial_price, 
                        self.df_col_price_per_m2, 
                        self.df_col_rooms, 
                        self.df_col_m2]

        self.df_cols_numeric_extended = [self.df_col_growth, 
                        self.df_col_initial_price, 
                        self.df_col_price_per_m2, 
                        self.df_col_rooms, 
                        self.df_col_size_in_m2, 
                        self.df_col_extended_size_in_m2]

        self.hdrs = {'Connection': 'keep-alive',
                        'method': 'GET',
                        'scheme': 'https',
                        'Expires': '-1',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) \
                            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.3'}

    def get_page_numbers(self, url_sold_apartments, MAX_OBJECTS_PER_PAGE = 34):
        page = requests.get(url_sold_apartments)
        soup = BeautifulSoup(page.content, 'lxml')
        data = soup.find_all('div', class_ = 'search-list__pagination-summary')

        try:
            number_of_objects = int(data[0].text[-(len(data[0].text)-3 - data[0].text.rfind("av")):])
        except:
            number_of_objects = 0

        number_of_pages = int(np.ceil(number_of_objects/MAX_OBJECTS_PER_PAGE))
        
        return number_of_pages

    def get_page_links(self, max_sold_date, min_sold_date):
        url_sold_apartments = self.base_url + '/slutpriser/' + self.region + '/' + self.region_id + '/?maxSoldDate=' + max_sold_date + '&minSoldDate=' + min_sold_date + '&objectType=Lägenhet'
        page_numbers = self.get_page_numbers(url_sold_apartments)
        return [url_sold_apartments + f'&page={i}' for i in range(1, page_numbers)]

    def scrape_html_apartments(self, link):
        '''
        :return: scrapes the content of the class URL,
                   using headers defined in the init function,
                   returning a byte string of html code.
        '''
        print(f'Scarping page: {link}')
        page = requests.get(link, headers = self.hdrs)
        soup = BeautifulSoup(page.content, 'lxml')
        html_lists = soup.find_all('li', class_ = 'search-list__item')
        return html_lists

    def sleeper(self, seconds = random.randint(3,5), i = -1):
        start_time = time.time()
        if i != -1:
            print(f'Sleep time in seconds: {seconds}')
        time.sleep(seconds)
        return time.time() - start_time

    def get_object_links_from_html_lists(self, html_lists):
        return [self.base_url + object.a.get('href') for object in html_lists if len(object.text.split("\n")) <= 17]

    def get_objects_from_html_lists(self, html_lists):
        return [object.text.split("\n") for object in html_lists if len(object.text.split("\n")) <= 17]
        
    def remove_empty_values_from_objects(self, objects):
        return [[objects[i][j] for j in range(0, len(objects[i])) if objects[i][j] != ''] for i in range(0, len(objects))]

    def create_pandas_dataframe(self, objects, object_links):
        df = pd.DataFrame(objects, columns = self.df_scraped_cols)
        df[self.df_col_link] = object_links
        return df 

    def drop_df_cols(self, df):
        try:
            df = df[self.df_cols_extended]
        except:
            df = df[self.df_cols]
            pass
        return df

    def expand_df_cols(self, df):
        df[self.df_cols_split_room_m2] = df.RoomM2.str.split(',', 1, expand = True)
        df[self.df_cols_split_type_region] = df.TypeRegion.str.split(',', 1, expand = True)
        try:
            df[self.df_cols_split_m2] = df.m2.str.split('+', 1, expand = True)
        except:
            df = df.rename(columns = {self.df_col_m2 : self.df_col_size_in_m2})
            df[self.df_col_extended_size_in_m2] = np.nan
            pass
        return df

    def clean_df_col_growth(self, df):
        chars_to_remove = ['%', '+', '-']
        df = self.clean_df_col(df, chars_to_remove, self.df_col_growth)
        return df

    def clean_df_cols_price_sice(self, df):
        chars_to_remove = ['kr/m²', 'rum', ' ']
        try:
            df = self.clean_df_col(df, chars_to_remove, self.df_cols_numeric_extended)
        except:
            df = self.clean_df_col(df, chars_to_remove, self.df_cols_numeric)
            pass
        return df

    def clean_df_col(self, df, chars_to_remove, cols):
        regular_expression = '[' + re.escape (''. join (chars_to_remove)) + ']'
        df[cols] = df[cols].replace(regular_expression, '', regex = True)
        return df

    def replace_nans_with_zeros(self, df):
        try:
            df[self.df_cols_numeric_extended] = df[self.df_cols_numeric_extended].fillna(0)
        except:
            df[self.df_cols_numeric] = df[self.df_cols_numeric].fillna(0)
            pass
        return df
        
    def set_df_col_to_numeric(self, df):
        try:
            df[self.df_cols_numeric_extended] = df[self.df_cols_numeric_extended].apply(pd.to_numeric, errors = 'coerce')
        except:
            df[self.df_cols_numeric] = df[self.df_cols_numeric].apply(pd.to_numeric, errors = 'coerce')
            pass
        return df

    def replace_df_comma_dot(self, df):
        return df.str.replace(',', '.')

    def set_df_col_to_date(self, df):
        df[self.df_col_date_sold] = df[self.df_col_date_sold].apply(lambda x: pd.to_datetime(dateparser.parse(x)))
        return df


class BooliApartmentsGranular(BooliApartments):
    
    def __init__(self):
        BooliApartments.__init__(self)
        self.df_col_price_change = 'PriceChangeInSek'
        self.df_col_days_on_booli = 'DaysOnBooli'
        self.df_col_floor_number = 'FloorNumber'
        self.df_col_monthly_charge = 'MonthlyCharge'
        self.df_col_type = 'Type'
        self.df_col_built_year = 'BuiltYear'
        self.df_col_brf = 'BRF'

    def scrape_html_apartments_granular(self, link):
        '''
        :return: scrapes the content of the class URL,
                   using headers defined in the init function,
                   returning a byte string of html code.
        '''
        print(f'Scarping page: {link}')
        page = requests.get(link, headers = self.hdrs)
        soup = BeautifulSoup(page.content, 'lxml')
        html_values = soup.find_all('div', class_ = '_18w8g')
        html_headers = soup.find_all('div', class_ = '_2soQI')
        return html_headers, html_values

    def flatten_object_list(self, objects):
        return [item for sublist in objects for item in sublist]

    def create_pandas_dataframe(self, values, headers, object_link):
        df = pd.DataFrame(values, columns = headers)
        df[self.df_col_link] = object_link
        return df 

    def clean_df_col_monthlyCharge_floorNumber(self, df):
        chars_to_remove = [' kr/mån', 'tr']
        df = self.clean_df_col(df, chars_to_remove, [self.df_col_monthly_charge, self.df_col_floor_number])
        return df