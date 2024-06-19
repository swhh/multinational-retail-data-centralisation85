import pandas as pd

class DataCleaning:
    
    def clean_user_data(self, df):
        """Clean user data, removing duplicates, erroneous entries, clean up addresses/phone numbers"""
        clean_df = df.copy()
        clean_df.drop_duplicates(inplace=True)   # drop duplicates
        clean_df.drop(columns=['index'], inplace=True) # drop redundant index
        clean_df['date_of_birth'] = pd.to_datetime(clean_df['date_of_birth'], format='mixed', errors='coerce')
        clean_df['join_date'] = pd.to_datetime(clean_df['join_date'], format='mixed', errors='coerce')

        country_codes = clean_df['country_code'].replace('GGB', 'GB')
        country_codes = country_codes.apply(lambda x: x if x in ['GB', 'US', 'DE'] else pd.NA)
        clean_df['country_code'] = country_codes

        country = clean_df['country']
        country = country.apply(lambda x: x if x in ['Germany', 'United Kingdom', 'United States'] else pd.NA)
        clean_df['country'] = country
        
        clean_df['phone_number'] = self._clean_phone_numbers(clean_df['phone_number'])
        clean_df['address'] = self._clean_addresses(clean_df['address'])

        return clean_df



    def _clean_phone_numbers(self, phone_df):
        return phone_df.replace({r'\+4\d': '0',  r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True)
    
    def _clean_addresses(self, addresses):
        addresses = addresses.str.replace('\n', ',', regex=False)
        return addresses.str.upper()
    



