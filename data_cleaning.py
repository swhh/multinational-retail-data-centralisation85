import math
import numpy as np
import pandas as pd


CARD_PROVIDERS = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
       'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
       'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
INDEX_COLUMNS = ['index', 'level_0', 'Unnamed: 0', 'unnamed']
COUNTRY_CODES = ['GB', 'US', 'DE']
COUNTRIES = ['Germany', 'United Kingdom', 'United States']
BAD_STRING_REGEX = r'^(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{4,}$' # pattern to match any long random string of uppercase letters and numbers
UUID_REGEX = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' # pattern to match a valid uuid

def weight_conversion(weight_str):
    """Convert weight string to a weight float in kgs"""
    if type(weight_str) == float:
        return weight_str
    if weight_str.endswith('ml'):
        weight = float(weight_str[:-2])
        return round(weight / 1000, 2)
    elif weight_str.endswith('kg'):
        if 'x' in weight_str:
            nums = weight_str[:-2].split('x')
            if len(nums) == 2 and all(num.isnumeric() for num in nums):
                return round(float(nums[0]) * float(nums[1]), 2)
            else:
                return np.nan           
        weight = float(weight_str[:-2])
        return round(weight, 2)
    elif weight_str.endswith('g'):
        if 'x' in weight_str:
            nums = weight_str[:-1].split('x')
            if len(nums) == 2 and all(i.isnumeric() for i in nums):
                return round((float(nums[0]) * float(nums[1])) / 1000, 2)
            else:
                return np.nan  
        weight = float(weight_str[:-1])
        return round(weight / 1000, 2)
    elif weight_str.endswith('oz'):
        weight = float(weight_str[:-2])
        return round(weight / 35.274, 2)
    else:
        return np.nan

class DataCleaning:
    
    def clean_user_data(self, df):
        """Clean user data, removing duplicates, erroneous entries, clean up addresses/phone numbers"""
        clean_df = self._generic_clean(df)
        clean_df = self._datetime_conversion(clean_df) # convert datetime strings to datetime objects

        clean_df['user_uuid'] = self._replace_bad_strings(clean_df['user_uuid'])
        clean_df.dropna(subset=['user_uuid'], inplace=True) # remove all rows with no user_uuid

        country_codes = clean_df['country_code'].replace('GGB', 'GB')
        clean_df['country_code'] = country_codes.apply(lambda x: x if x in COUNTRY_CODES else np.nan)
        clean_df['country'] = clean_df['country'].apply(lambda x: x if x in COUNTRIES else np.nan)
        
        clean_df['phone_number'] = self._clean_phone_numbers(clean_df['phone_number'])
        clean_df['address'] = self._clean_addresses(clean_df['address'])
        return clean_df
    

    def clean_card_data(self, df):
        """clean the data to remove any erroneous values, 
        NULL values or errors with formatting"""
        # clean card numbers
        clean_df = df.copy()
        clean_df.card_number = self._clean_card_numbers(clean_df.card_number)
        # split out data contained in 'card_number expiry_date' column
        clean_df['card_number'] = clean_df.apply(self._split_wrapper('card_number'), axis=1) 
        clean_df['expiry_date'] = clean_df.apply(self._split_wrapper('expiry_date'), axis=1)
        # drop duplicates/remove null values
        clean_df = self._generic_clean(clean_df)
        # clean dates
        clean_df['expiry_date'] = self._replace_bad_strings(clean_df['expiry_date'])
        clean_df['date_payment_confirmed'] = pd.to_datetime(clean_df['date_payment_confirmed'], format='mixed', errors='coerce')
        #clean card_providers
        clean_df['card_provider'] = clean_df['card_provider'].apply(lambda x: x if x in CARD_PROVIDERS else np.nan)
        # clean card numbers     
        clean_df = clean_df.dropna(subset=['card_number'])
        return clean_df
    

    def clean_store_data(self, df):
         """clean the store data held in dataframe to remove any erroneous values, 
        NULL values or errors with formatting"""
         clean_df = self._generic_clean(df)
         clean_df['address'] = self._clean_addresses(clean_df['address'])
         for column_name in clean_df.columns:
            if column_name != 'store_code':
               clean_df[column_name] = self._replace_bad_strings(clean_df[column_name])
         clean_df['opening_date'] = pd.to_datetime(clean_df['opening_date'], format='mixed', errors='coerce')
         clean_df['continent'] = clean_df['continent'].str.replace('ee', '')
         clean_df['country_code'] = clean_df['country_code'].apply(lambda x: x if x in COUNTRY_CODES else np.nan)
         clean_df['staff_numbers'] = clean_df['staff_numbers'].apply(lambda x: int(''.join(filter(str.isdigit, x))) if type(x) is str else np.nan)
         return clean_df
    
    @staticmethod
    def convert_product_weights(df):
        """take the products DataFrame as an argument and return the products DataFrame, 
        standardise measurements to kg; the DataFrame the weights all have different units.
        Convert them all to a decimal value representing their weight in kg"""
        return df.apply(weight_conversion)

    def clean_products_data(self, df):
        """clean the DataFrame of any additional erroneous values"""
        clean_df = self._generic_clean(df)
        for column in df.columns:
            clean_df[column] = self._replace_bad_strings(column)
        clean_df.dropna(subset=['product_code', 'uuid', 'product_name'], inplace=True)
        # convert cols to the right data types
        clean_df['date_added'] = pd.to_datetime(clean_df['date_added'], format='mixed', errors='coerce')
        clean_df['product_price'] = pd.to_numeric(clean_df['product_price'].str.replace('£', ''), errors='coerce')
        clean_df['weight'] = self._convert_product_weights(clean_df['weight'])
        return clean_df
    
    def clean_orders_data(self, df):
        """clean the orders table data.
       remove the columns, first_name, last_name and 1 to have the table in the correct form before uploading to the database."""
        clean_df = self._generic_clean(df)
        return clean_df
    
    def clean_date_events(self, df):
        """clean the date_events table, removing NULL and erroneous values"""
        clean_df = self._generic_clean(df)
        clean_df['date_uuid'] = self._filter_uuids(clean_df['date_uuid']) # map to nan any non-uuids
        clean_df.dropna(subset=['date_uuid'], inplace=True) # remove records with no date_uuids
        for column_name in clean_df.columns:
            clean_df[column_name] = self._replace_bad_strings(clean_df[column_name])
        for column_name in ['year', 'month', 'day']:
            clean_df[column_name] = clean_df[column_name].astype(str).apply(lambda x: x if x.isnumeric() else np.nan)      
        return clean_df
       
    @staticmethod
    def _clean_phone_numbers(phone_df):
        """Clean phone numbers removing spaces and country codes"""
        return phone_df.replace({r'\+4\d': '0',  r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True)
    
    @staticmethod
    def _clean_addresses(self, addresses):
        '''Take Pandas series addresses and return standardised address strings as series'''
        addresses = addresses.str.replace('\n', ',', regex=False)
        return addresses.str.upper()
    @staticmethod 
    def _clean_card_numbers(self, card_numbers):
        """Filter out invalid card number strings from pandas series card_numbers"""
        clean_card_numbers = card_numbers.astype(str)
        clean_card_numbers = card_numbers['card_number'].str.replace(r'[^\d]', '', regex=True)
        return clean_card_numbers.apply(lambda x: x if len(x) > 5 else np.nan)
    
    @staticmethod
    def _split_wrapper(column):
        """Wrapper function that uses closure to produce a function that can help split the """
        def split_row(row):
            string = str(row['card_number expiry_date'])
            if string != np.nan:
               strings = string.split(' ')
               if len(strings) == 2:
                  card_num, expiry_date = strings
                  if column == 'card_number':
                      if card_num.isnumeric():
                         return card_num
                  elif column == 'expiry_date':
                      if expiry_date[0].isnumeric():
                          return expiry_date                  
            return row[column]
        return split_row
    
    @staticmethod
    def _replace_bad_strings(df):
        """Find junk string values in Pandas series df and replace with nan"""
        clean_df = df.astype(str)
        clean_df = clean_df.str.strip()
        clean_df = clean_df.replace({'N/A': np.nan, 'NULL': np.nan, BAD_STRING_REGEX: np.nan}, regex=True)
        return clean_df
    
    @staticmethod
    def _datetime_conversion(df, format='mixed'):
        """convert datetime strings to datetimes for all date columns in df"""
        clean_df = df.copy()
        for header in clean_df.columns:
            if header.startswith('date_') or header.endswith('_date'):
                clean_df[header] = pd.to_datetime(clean_df[header], format=format, errors='coerce')
        return clean_df
    
    @staticmethod
    def _filter_uuids(df):
        """Take Pandas series as input and replace all non-uuids with nan; return series"""
        mask = df.str.contains(UUID_REGEX, regex=True)
        return df.where(mask, np.nan)
    
    @staticmethod
    def _generic_clean(df):
        """Get rid of duplicates, index columns and null rows and columns"""
        clean_df = df.drop_duplicates() # drop duplicate rows
        clean_df = clean_df.dropna(how='all')  # drop empty rows
        threshold = math.floor(0.9 * len(clean_df))
        clean_df = clean_df.dropna(axis=1, thresh=threshold) # drop cols with mostly null values
        clean_df.drop(columns=INDEX_COLUMNS, inplace=True, errors='ignore') # drop extra index cols
        return clean_df