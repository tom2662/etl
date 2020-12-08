import pandas as pd
import numpy as np
# import MySQLdb
from sqlalchemy import create_engine

crypto_df = pd.read_csv('crypto-markets.csv')
assetsCode = ['BTC','ETH','XRP','LTC']

# coverting open, close, high and low price of crypto currencies into GBP values since current price is in Dollars
# if currency belong to this list ['BTC','ETH','XRP','LTC']
crypto_df['open'] = crypto_df[['open', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in assetsCode else np.nan, axis=1)
crypto_df['close'] = crypto_df[['close', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in assetsCode else np.nan, axis=1)
crypto_df['high'] = crypto_df[['high', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in assetsCode else np.nan, axis=1)
crypto_df['low'] = crypto_df[['low', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in assetsCode else np.nan, axis=1)

# dropping rows with null values by asset column
crypto_df.dropna(inplace=True)

# reset the data frame index
crypto_df.reset_index(drop=True ,inplace=True)
#drop unimportant column
crypto_df.drop(labels=['slug', 'ranknow', 'volume', 'market', 'close_ratio', 'spread'], inplace=True, axis=1)

# mydb = MySQLdb.connect(host='localhost',
#     user='root',
#     passwd='qwerty1234',
#     db='etldb')

# # Drop a table name Crypto if it exists already
# try:
#     mydb.query('DROP TABLE IF EXISTS `Crypto` ')
# except Exception as e:
#     raise(e)
# finally:
#     print('Table dropped')

    
# # Create a new Table named as Crypto
# try:
#     mydb.query('''
#          CREATE TABLE Crypto
#          (ID         INTEGER PRIMARY KEY,
#          NAME        TEXT    NOT NULL,
#          Date        datetime,
#          Open        Float DEFAULT 0,
#          High        Float DEFAULT 0,
#          Low         Float DEFAULT 0,
#          Close       Float DEFAULT 0);''')
#     print ("Table created successfully");
# except Exception as e:
#     print(str(e))
#     print('Table Creation Failed!!!!!')
# finally:
#     mydb.close() # this closes the database connection


# print(crypto_df.head())


engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="qwerty1234",
                               db="etldb"))

try:
    crypto_df.to_sql(name='crypto',con=engine,index=False, if_exists='replace')
    print('Sucessfully written to Database')

except Exception as e:
    print(e)
    print('Fail written to Database')



