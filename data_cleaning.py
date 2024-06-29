import pandas as pd

import math
import numpy as np

EXPIRY_DATE_FORMAT = '%m/%y'
CARD_PROVIDERS = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
       'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
       'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']

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
        clean_df = self._datetime_conversion(clean_df)

        country_codes = clean_df['country_code'].replace('GGB', 'GB')
        clean_df['country_code'] = country_codes.apply(lambda x: x if x in ['GB', 'US', 'DE'] else pd.NA)

        country = clean_df['country']
        clean_df['country'] = country.apply(lambda x: x if x in ['Germany', 'United Kingdom', 'United States'] else pd.NA)
        
        clean_df['phone_number'] = self._clean_phone_numbers(clean_df['phone_number'])
        clean_df['address'] = self._clean_addresses(clean_df['address'])

        return clean_df
    

    def clean_card_data(self, df):
        """clean the data to remove any erroneous values, 
        NULL values or errors with formatting"""
        # drop duplicates/remove null values
        clean_df = self._generic_clean(df)
        clean_df = clean_df.dropna(subset=['card_number'])
        # clean dates
        clean_df['expiry_date'] = pd.to_datetime(clean_df['expiry_date'], format=EXPIRY_DATE_FORMAT, errors='coerce')
        clean_df['date_payment_confirmed'] = pd.to_datetime(clean_df['date_payment_confirmed'], format='mixed', errors='coerce')
        #clean card_providers
        clean_df['card_provider'] = clean_df['card_provider'].apply(lambda x: x if x in CARD_PROVIDERS else np.nan)
        # clean card numbers     
        clean_df.card_number = self._clean_card_numbers(clean_df.card_number)
        return clean_df
    

    def clean_store_data(self, df):
         """clean the store data held in dataframe to remove any erroneous values, 
        NULL values or errors with formatting"""
         clean_df = self._generic_clean(df)
         clean_df['address'] = self._clean_addresses(clean_df['address'])
         clean_df['opening_date'] = pd.to_datetime(clean_df['opening_date'], format='mixed', errors='coerce')
         clean_df['locality'] = self._replace_bad_strings(clean_df['locality'])
         clean_df['country_code'] = self._replace_bad_strings(clean_df['country_code'])
         clean_df['continent'] = self._replace_bad_strings(clean_df['continent'])
         clean_df['continent'] = clean_df['continent'].str.replace('ee', '')
         clean_df['staff_numbers'] = pd.to_numeric(clean_df['staff_numbers'], errors='coerce', downcast='integer')
         return clean_df
    

    def convert_product_weights(self, df):
        """take the products DataFrame as an argument and return the products DataFrame, 
        standardise measurements to kg; the DataFrame the weights all have different units.
        Convert them all to a decimal value representing their weight in kg"""
        return df.apply(weight_conversion)

    def clean_products_data(self, df):
        """clean the DataFrame of any additional erroneous values"""
        clean_df = self._generic_clean(df)
        clean_df['date_added'] = pd.to_datetime(clean_df['date_added'], format='mixed', errors='coerce')
        clean_df['category'] = self._replace_bad_strings(clean_df['category'])
        clean_df['removed'] = self._replace_bad_strings(clean_df['removed'])
        clean_df['product_price'] = pd.to_numeric(clean_df['product_price'].str.replace('£', ''), errors='coerce')
        clean_df['weight'] = clean_df['weight'].apply(weight_conversion)
        return clean_df
    
    def clean_orders_data(self, df):
        """clean the orders table data.
       remove the columns, first_name, last_name and 1 to have the table in the correct form before uploading to the database."""
        clean_df = self._generic_clean(df)
        return clean_df
    
    def clean_date_events(self, df):
        """clean the date_events table, removing NULL and erroneous values"""
        clean_df = self._generic_clean(df)
        for column_name in clean_df.columns:
            clean_df[column_name] = self._replace_bad_strings(clean_df[column_name])
        years = df.year.astype(str)
        clean_df.year = years.apply(lambda x: x if x.isnumeric() else np.nan)
        return clean_df
       

    def _clean_phone_numbers(self, phone_df):
        return phone_df.replace({r'\+4\d': '0',  r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True)
    
    def _clean_addresses(self, addresses):
        '''Take Pandas series addresses and return standardised address strings as series'''
        addresses = addresses.str.replace('\n', ',', regex=False)
        return addresses.str.upper()

    def _clean_card_numbers(self, card_numbers):
        """Filter out invalid card number strings from pandas series card_numbers"""
        clean_card_numbers = card_numbers.astype(str)
        return clean_card_numbers.apply(lambda x: x if len(x) > 5 and x.isdigit() else np.nan)
    
    def _replace_bad_strings(self, df):
        """Find junk string values in Pandas series df and replace with nan"""
        new_df = df.astype(str)
        new_df = new_df.replace({'N/A': np.nan, 'NULL': np.nan, '^(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]*$': np.nan}, regex=True)
        return new_df
    
    def _datetime_conversion(self, df, format='mixed'):
        """convert datetime strings to datetimes for all date columns in df"""
        clean_df = df.copy()
        for header in clean_df.columns.values.tolist():
            if header.startswith('date_') or header.endswith('_date'):
                clean_df[header] = pd.to_datetime(clean_df[header], format=format, errors='coerce')
        return clean_df

    
    def _generic_clean(self, df):
        """Get rid of duplicates, index columns and null rows and columns"""
        clean_df = df.drop_duplicates()
        clean_df = clean_df.dropna(axis=1, how='all')
        clean_df = clean_df.dropna(how='all')
        threshold = math.floor(0.9 * len(clean_df))
        clean_df = clean_df.dropna(axis=1, thresh=threshold)
        clean_df.drop(columns=['index'], inplace=True, errors='ignore')
        clean_df.drop(columns=['unnamed'], inplace=True, errors='ignore')
        clean_df.drop(columns=['Unnamed: 0'], inplace=True, errors='ignore')
        clean_df.drop(columns=['level_0'], inplace=True, errors='ignore')
        return clean_df
    
    


    



