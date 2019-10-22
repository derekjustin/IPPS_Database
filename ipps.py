'''
Authors: Derek Holsapple, Justin Strelka
Date: 10/20/2019
Project: IPPS_Database
'''

import pymysql
import os
from glob import glob
import pandas as pd
from sqlalchemy import create_engine

class ipps:
    
    def getCSVfilefromCwD():
        # Get current working directory from os.
        PATH = os.getcwd()   
        EXT = "*.csv"
        
        # Create file array of all .csv files in directory
        all_csv_files = [file
            for path, subdir, files in os.walk(PATH)
            for file in glob(os.path.join(path, EXT))]
        
        return all_csv_files
    
    def loadCSVtoDf():
        # Get csv list and create raw_input table
        csvList = ipps.getCSVfilefromCwD()
        raw_input = pd.read_csv( csvList[0] ) 
        
        # Place raw_input into raw_df dataframe
        raw_df = raw_input.loc[:,['Provider Id',
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
        # Match the column names of the sql table names    
        raw_df.rename(columns = {'Provider Id' :'providerId', 
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
        
        # Split the selected columns to meet 2nd normal form and place in raw_df dataframe                    
        raw_df[['dRgKey','dRgDescription']] = raw_input['DRG Definition'].str.split(' - ',expand=True)
        raw_df[['referralRegionState','referralRegionDescription']] = raw_input['Hospital Referral Region Description'].str.split(' - ',expand=True)
        return raw_df

    # Create porvidersDF dataframe to become providers SQL table without duplicates
    def getProvidersDF(raw_df):        
        providers_df = raw_df.loc[:,['providerId',
                                    'providerName',
                                    'providerStreetAddress', 
                                    'providerCity', 
                                    'providerState',
                                    'providerZipCode',
                                    'referralRegionState',
                                    'referralRegionDescription'
                                    ]]
        return providers_df.drop_duplicates()

    # Create drg_df dataframe to become drg SQL table without duplicates
    def getdRgDF(raw_df):
        drg_df = raw_df.loc[:,['dRgKey',
                                'dRgDescription'
                                ]]
        return drg_df.drop_duplicates()

    # Create provider_cond_converage_df to become 
    # providercondcoverage SQL table without duplicates
    def getProviderCondCoverage(raw_df):
        provider_cond_coverage_df = raw_df.loc[:,['providerId',
                                                'dRgKey',
                                                'totalDischarges',
                                                'averageCoveredCharges',
                                                'averageTotalPayments',
                                                'averageMedicarePayments'
                                                ]]
        return provider_cond_coverage_df.drop_duplicates()

    # Driver to push dataframes into SQL tables
    def pushToSQL():
        # User Credentials
        server = 'mysql+pymysql://{user}:{pw}@localhost/{db}'
        database = 'ipps'
        user = 'ipps'
        password = '12345'

        # Create a engine to connect to mySQL
        engine = create_engine(server.format(user = user,
                               pw = password,
                               db = database)) 
        # Notify user if MySQL connection was a success.    
        if (engine):
            print('Connection to MySQL database', database, 'was successful!')    
        # Get dataframes
            raw_df = ipps.loadCSVtoDf()            
            providers_df = ipps.getProvidersDF(raw_df)
            drg_df = ipps.getdRgDF(raw_df)
            provider_cond_coverage_df = ipps.getProviderCondCoverage(raw_df)
                 
            # Push dataframes to SQL tables
            # TODO: REMOVE raw_df.to_sql, the table is only for developing reference
            raw_df.to_sql('raw_df', con = engine, if_exists = 'append', chunksize = 1000 , index = False)
            providers_df.to_sql('providers', con = engine, if_exists = 'append', chunksize = 1000 , index = False)
            drg_df.to_sql('drg', con = engine, if_exists = 'append', chunksize = 1000 , index = False)
            provider_cond_coverage_df.to_sql('providercondcoverage', con = engine, if_exists = 'append', chunksize = 1000 , index = False)
    

        else:
            print('Connection to MySQL database', database, 'was NOT successful!')

        # close the connection
        engine.dispose()

# Call Push to SQL driver
ipps.pushToSQL()



 