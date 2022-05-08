import pandas as pd
from database_pipeline import DatabaseConnector, Database
from datetime import datetime,timedelta
from sqlalchemy.types import Float,DateTime,NVARCHAR,Integer,Time
mysql_dtype_dict={'object':NVARCHAR(length=255),'int32':Integer(),'int64':Integer(),'float64':Float(),'datetime64[ns]':DateTime()}

def convert_date(date):
    return date.strftime('%Y%m%d')
db_name='price_info'
table_name='stock_prices'
connector_engine=DatabaseConnector(db_name).engine
start_date=datetime(2020,3,23)
end_date=datetime(2020,3,23)

date=start_date
columns=['time', 'code', 'open', 'close', 'high', 'low', 'volume', 'money']
dtypes = ['datetime64[ns]', 'object', 'float64', 'float64', 'float64', 'float64', 'float64', 'float64']
dtype_dict=dict(zip(columns,map(mysql_dtype_dict.get, dtypes)))

terms_amount=10
term_unit='day'
database = Database(connector_engine, table_name)
while date <= end_date:
    name='Stock Price '+convert_date(date)+'.csv'

    try:
        df=pd.read_csv('Stock Price 20200416\\'+name)[columns]
        df['time']=pd.to_datetime(df['time'])
        print("read complete.")
        database.upload_data(df, table_name, if_exists='append', dtype=dtype_dict)
        print(date)
    except Exception as e:
        print(e.args)
        pass
    
    date=date+timedelta(days=1)
#%% Upload to database


