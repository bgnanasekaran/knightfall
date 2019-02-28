import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import random
import datetime

# Sample Code to normalize a sample.json into a DataFrame, iterate it to create a large dataset .parquet file #
count = 0
print("start time ::" + str(datetime.datetime.now()))
while count < 100001:
    d = {"header": {"uuid": str(uuid.uuid4()), "type": "store-transaction", "created": 1461024358442,
                    "modified": 1461024358442, "alias": "245^20160408^63^8794", "transient": False,
                    "sku": random.randint(1, 1000001)}}
    df1 = pd.io.json.json_normalize(d)
    # way to split the normalized file by removing the 'parentNode' in a 'parentNode.childNode' normalized form
    # df1.columns = df1.columns.map(lambda x: x.split(".")[-1])
    new_df1 = pd.DataFrame(df1)
    if count == 0:
        new_df2 = new_df1
    else:
        new_df2 = new_df2.append(new_df1)
    count += 1
print("data creation done")
# creates a table form using pandas
table = pa.Table.from_pandas(new_df2)
# create a parquet file using pyarrow.parquet
pq.write_table(table, 's3_sample_data.parquet')
print("end time ::" + str(datetime.datetime.now()))
print("done")

# Documents from Web/Stack overflow - GoodReads
# Install Document

# https://arrow.apache.org/docs/python/install.html

# pandas
# https://stackoverflow.com/questions/38376351/no-module-named-pandas-in-pycharm
# https://stackoverflow.com/questions/47191675/pandas-write-dataframe-to-parquet-format-with-append
# https://stackoverflow.com/questions/42597208/how-to-save-data-in-parquet-format-and-append-entries
# https://stackoverflow.com/questions/34341974/nested-json-to-pandas-dataframe-with-specific-format
# https://stackoverflow.com/questions/36526282/append-multiple-pandas-data-frames-at-once
# https://stackoverflow.com/questions/19124601/pretty-print-an-entire-pandas-series-dataframe
# https://stackoverflow.com/questions/36054321/parallel-processing-in-pandas-python
# https://stackoverflow.com/questions/20887555/dead-simple-example-of-using-multiprocessing-queue-pool-and-locking
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html
# https://stackoverflow.com/questions/37374568/how-to-load-a-json-into-a-pandas-dataframe
# http://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html
# https://stackoverflow.com/questions/41168558/python-how-to-convert-json-file-to-dataframe
# https://stackoverflow.com/questions/20643437/create-pandas-dataframe-from-json-objects
# https://pandas.pydata.org/pandas-docs/version/0.17.0/generated/pandas.io.json.json_normalize.html
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.io.json.json_normalize.html


# pyarrow
# https://stackoverflow.com/questions/47113813/using-pyarrow-how-do-you-append-to-parquet-file

# python
# https://www.learnpython.org/
# https://stackoverflow.com/questions/534839/how-to-create-a-guid-uuid-in-pythonen/Loops
# https://stackoverflow.com/questions/37049289/how-do-i-change-a-uuid-to-a-string
# https://www.pythonforbeginners.com/loops/for-while-and-nested-loops-in-python
# https://tecadmin.net/get-current-date-time-python/
# https://stackoverflow.com/questions/11559062/concatenating-string-and-integer-in-python

# table2 = pq.read_table('example.parquet') print(table2.to_pandas()) d1 = {"header": {"uuid": str(uuid.uuid4()),
# "type":"store-transaction", "created":1461024358442,"modified":1461024358442,"alias":"245^20160408^63^8794",
# "transient":False,"sku":sku}} df2 = pd.io.json.json_normalize(d1) new_df2 = pd.DataFrame(df2) with
# pd.option_context('display.max_rows', None, 'display.max_columns', None):print(new_df1)
