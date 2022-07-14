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
start_date=datetime(2020,2,27)
end_date=datetime(2020,4,23)

date=start_date
columns=['time', 'code', 'open', 'close', 'high', 'low', 'volume', 'money']
dtypes = ['datetime64[ns]', 'object', 'float64', 'float64', 'float64', 'float64', 'float64', 'float64']
dtype_dict=dict(zip(columns,map(mysql_dtype_dict.get, dtypes)))

database = Database(connector_engine, table_name)
database.create_table()
#database.operate_by_query("delete from stock_prices where time>'2020-03-23 00:00:00' and time<'2020-03-24 00:00:00'")
#print("Delete complete.")
while date <= end_date:
    name='Stock Price '+convert_date(date)+'.csv'

    try:
        df=pd.read_csv('Stock Price 20200416\\' + name)[columns]
        df['time']=pd.to_datetime(df['time'])
        print(str(date) + "data read complete.")
        database.upload_data(df, table_name, if_exists='append', dtype=dtype_dict)
        print(str(date) + "data upload complete.")
    except Exception as e:
        print(e.args)
        pass
    
    date=date+timedelta(days=1)
#%% Upload to database


