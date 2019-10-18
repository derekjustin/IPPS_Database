import pymysql
import os
from glob import glob
import pandas as pd
from sqlalchemy import create_engine

class ipps:

    '''
    getCSVfilefromCwD()
    
    Locate the current working directory.  Make a list of all the csv files withing the current working directory
    

    return    list:  List of strings containing the path of csv files
    '''
    
    def getCSVfilefromCwD():
        
        # Find the current working directory 
        PATH = os.getcwd()   
        EXT = "*.csv"
        
        # List of all csv files
        all_csv_files = [file
            for path, subdir, files in os.walk(PATH)
            for file in glob(os.path.join(path, EXT))]
        
        return all_csv_files
    
    
    '''
    loadCSVtoDf( path )
    
    Create a pandas dataframe from the csv file
    
    
    return    Dataframe:  Dataframe of the first csv from the current working directory
    '''
    
    def loadCSVtoDf():
        
        # Get the list of all CSVs from current working directory
        csvList = ipps.getCSVfilefromCwD()

        raw_df = pd.read_csv( csvList[0] ) 

        # Copy initial columns over to the frame that are already in 2nd NF
        SecondNF_df = raw_df.loc[:,['Provider Id',
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
        SecondNF_df.rename(columns = {'Provider Id' :'providerId', 
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
        
        # Split the selected columns to meet 2nd normal form                     
        SecondNF_df[['dRgKey','dRgDescription']] = raw_df['DRG Definition'].str.split(' - ',expand=True)
        SecondNF_df[['referralRegionState','referralRegionDescription']] = raw_df['Hospital Referral Region Description'].str.split(' - ',expand=True)

        #TODO: We need to break apart the pandas dataframe into separate 
                # columns determined by the 3rd normal form.  I think we should 
                # create separate functions to break apart the frame to meet our 
                # rubric of 3rd NF
        return SecondNF_df

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

    def pushToSQL():
            server = 'localhost'
            database = 'ipps'
            user = 'ipps'
            #TODO: WE MAY NEED TO SAVE PW IN SOME OTHER FILE NEED TO ASK HIM THE QUESTION
            password = '12345'
    
            # Get 2nd NF dataFrame
            raw_df = ipps.loadCSVtoDf()
            # connects to the database
#            conn = pymysql.connect(host = server, user = user, password = password, db = database)
            
            providers = ipps.getProvidersDF(raw_df)
            
            # Create a engine to connect to mySQL
            engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="ipps",
                               pw="12345",
                               db="ipps")) 
            
            
            # Create a SQL table named Providers from the 2ndNF pandas dataFrame
            raw_df.to_sql('raw_df', con = engine, if_exists = 'append', chunksize = 1000 , index = False)

            providers.to_sql('providers', con = engine, if_exists = 'append', chunksize = 1000 , index = False)
            
            #TODO: Not Sure if I have to close the connection of the engine i.e engine.close()
            
            if (engine):
                print('Connection to MySQL database', database, 'was successful!')
    
            
            #GOOD STUFF TO KEEP
    
            # new row
            #cursor = conn.cursor()
            #sql = 'INSERT INTO Employees VALUES (%s, %s, %s)'
            #cursor.execute(sql, (4, 'Jose Caipirinha', 65000))
            #conn.commit()
    
    
            # run a simple query
            #sql = 'SELECT id, name, sal FROM Employees'
            #cursor = conn.cursor()
            #cursor.execute(sql)
            #for id, name, sal in cursor:
            #    print(id, name, sal)
    
    
    
            
            # closes the connection
            print('Bye!')
            #engine.close()


ipps.pushToSQL()



 