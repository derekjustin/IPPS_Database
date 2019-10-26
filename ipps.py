'''
Authors: Derek Holsapple, Justin Strelka
Date: 10/26/2019
Project: IPPS_Database

Description: Python driver to import ipps.csv data 
file. Use pandas package to normalize csv data into 
3rd normal form. Pass 3rd nomalized form dataframes 
to SQL database.
'''

import os
from glob import glob
import pandas as pd
from sqlalchemy import create_engine

# Database credentials
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
SERVER = "localhost"
DATABASE = "ipps"

class ipps:
    
    
    def getCSVfilefromCwD():
            
        '''
        HELPER FUNCTION
        
        Create a list of all the csv files from the current working directory.
        
        @return  all_csv_files    -list:str, paths of csv files 
        '''
        
        # Get current working directory from os.
        PATH = os.getcwd()   
        EXT = "*.csv"
        
        # Create file array of all .csv files in directory
        all_csv_files = [file
            for path, subdir, files in os.walk(PATH)
            for file in glob(os.path.join(path, EXT))]
        
        return all_csv_files
       

    def loadCSVtoDf():
        
        '''
        HELPER FUNCTION
        
        Create a dataframe of the IPPS dataset into 2nd normalized form.
        
        @return  ipps_2NF_df    -dataframe, 2nd normalized form of IPPS dataset 
        '''
        
        # Create raw_df from CSV inputs, assign datatypes to columns
        csvList = ipps.getCSVfilefromCwD()
        raw_df = pd.read_csv( csvList[0], 
            dtype={'Provider Id': int,
                    'Provider Name': str,
                    'Provider Street Address': str,
                    'Provider City': str,
                    'Provider State': str,
                    'Provider Zip Code': int,
                    ' Total Discharges ': int,
                    ' Average Covered Charges ': float,
                    ' Average Total Payments ': float,
                    'Average Medicare Payments': float}) 
        
        ipps_2NF_df = raw_df.loc[:,['Provider Id',
                                    'Provider Name',
                                    'Provider Street Address', 
                                    'Provider City', 
                                    'Provider State',
                                    'Provider Zip Code',
                                    ' Total Discharges ',
                                    ' Average Covered Charges ',
                                    ' Average Total Payments ',
                                    'Average Medicare Payments'
                                    ]]

        # Clean the column names and eliminate white spaces from original import
        # Match the column names to sql DB attribute names    
        ipps_2NF_df.rename(columns = {'Provider Id' :'providerId', 
                                      'Provider Name':'providerName', 
                                      'Provider Street Address':'providerStreetAddress',
                                      'Provider City':'providerCity',
                                      'Provider State':'providerState',
                                      'Provider Zip Code':'providerZipCode',
                                      ' Total Discharges ':'totalDischarges',
                                      ' Average Covered Charges ':'averageCoveredCharges',
                                      ' Average Total Payments ':'averageTotalPayments',
                                      'Average Medicare Payments':'averageMedicarePayments'
                                      }, inplace = True)
        
        # Split the selected columns to meet 2nd normal form and place in ipps_2NF_df dataframe                    
        ipps_2NF_df[['dRgKey','dRgDescription']] = \
                raw_df['DRG Definition'].str.split(' - ',expand=True)
        ipps_2NF_df[['referralRegionState','referralRegionDescription']] = \
                raw_df['Hospital Referral Region Description'].str.split(' - ',expand=True)
        
        #Create a key referralRegionId for the referral region state and referral region description
        ipps_2NF_df['referralRegionId'] = ipps_2NF_df.groupby(['referralRegionState',
                                                 'referralRegionDescription']).ngroup()
        
        return ipps_2NF_df
    
    
    
    def get3NFReferralRegionDF(ipps_2NF_df):  
        
        '''
        HELPER FUNCTION
        
        Create dataframe to match hrr table in SQL.
        This will be one table of 3rd normalized form
        
        @return  referral_region_3NF_df    -dataframe, 3rd NF for table hrr
        '''
        
        referral_region_3NF_df = ipps_2NF_df.loc[:,['referralRegionId',
                                            'referralRegionState',
                                            'referralRegionDescription'
                                            ]]

        return referral_region_3NF_df.drop_duplicates()



    def get3NFProvidersDF(ipps_2NF_df):
        
        '''
        HELPER FUNCTION
        
        Create dataframe to match providers table in SQL.
        This will be one table of 3rd normalized form
        
        @return  providers_3NF_df    -dataframe, 3rd NF for table providers
        '''  
        
        providers_3NF_df = ipps_2NF_df.loc[:,['providerId',
                                    'providerName',
                                    'providerStreetAddress', 
                                    'providerCity', 
                                    'providerState',
                                    'providerZipCode',
                                    'referralRegionId'
                                    ]]
        return providers_3NF_df.drop_duplicates()



    def get3NFdRgDF(ipps_2NF_df):
        
        '''
        HELPER FUNCTION
        
        Create dataframe to match drg table in SQL.
        This will be one table of 3rd normalized form
        
        @return  drg_3NF_df    -dataframe, 3rd NF for table drg 
        '''
        
        drg_3NF_df = ipps_2NF_df.loc[:,['dRgKey',
                                'dRgDescription'
                                ]]
        return drg_3NF_df.drop_duplicates()


    def get3NFProviderCondCoverage(ipps_2NF_df):
        
        '''
        HELPER FUNCTION
        
        Create dataframe to match providercondcoverage table in SQL.
        This will be one table of 3rd normalized form
        
        @return  provider_cond_coverage_3NF_df    -dataframe, 3rd NF for table providercondcoverage 
        '''
        
        provider_cond_coverage_3NF_df = ipps_2NF_df.loc[:,['providerId',
                                                'dRgKey',
                                                'totalDischarges',
                                                'averageCoveredCharges',
                                                'averageTotalPayments',
                                                'averageMedicarePayments'
                                                ]]
        return provider_cond_coverage_3NF_df.drop_duplicates()



    def pushToSQL(SERVER,DATABASE,USER,PASSWORD):
        
        '''
        EXECUTION FUNCTION
        
        Driver to push dataframes into existing database SQL tables
        '''
        
        # User Credentials
        serverStr = 'mysql+pymysql://{user}:{pw}@{svr}/{db}'
        server = SERVER
        database = DATABASE
        user = USER
        password = PASSWORD

        # Create a engine to connect to mySQL
        engine = create_engine(serverStr.format(user = user,
                               pw = password,
                               svr = server,
                               db = database)) 

        # Get dataframes
        ipps_2NF_df = ipps.loadCSVtoDf()            
        providers_3NF_df = ipps.get3NFProvidersDF(ipps_2NF_df)
        drg_3NF_df = ipps.get3NFdRgDF(ipps_2NF_df)
        provider_cond_coverage_3NF_df = ipps.get3NFProviderCondCoverage(ipps_2NF_df)
        referral_region_3NF_df = ipps.get3NFReferralRegionDF(ipps_2NF_df)
                 
        # Push dataframes to SQL tables
        referral_region_3NF_df.to_sql('hrr', con = engine, if_exists = 'append', chunksize = 1000 , index = False)
        providers_3NF_df.to_sql('providers', con = engine, if_exists = 'append', chunksize = 1000 , index = False)
        drg_3NF_df.to_sql('drg', con = engine, if_exists = 'append', chunksize = 1000 , index = False)
        provider_cond_coverage_3NF_df.to_sql('providercondcoverage', con = engine, if_exists = 'append', chunksize = 1000 , index = False)    
    
        # Notify user if MySQL connection was a success.    
        if (engine):
            print('Connection to MySQL database', database, 'was successful!')    

        # close the connection
        engine.dispose()

# Call push to SQL driver
ipps.pushToSQL(SERVER,DATABASE,USER,PASSWORD)



 