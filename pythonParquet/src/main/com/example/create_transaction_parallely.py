import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import random
import datetime

count = 0
print("start time ::" + str(datetime.datetime.now()))
#while count < 100001:
d = {"uuid": str(uuid.uuid4()), "type": "store-transaction", "created": 1461024358442,
                    "modified": 1461024358442, "alias": "245^20160408^63^8794", "transient": False, "sku": random.randint(1, 1000001)}
df1 = pd.read_json(d)
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(df1)

#     df1 = pd.io.json.json_normalize(d)
#     new_df1 = pd.DataFrame(df1)
#     if count == 0:
#         new_df2 = new_df1
#     else:
#         new_df2 = new_df2.append(new_df1)
#     count += 1
# print("data creation done")
# table = pa.Table.from_pandas(new_df2)
# pq.write_table(table, 's3_sample_data.parquet')
# print("end time ::" + str(datetime.datetime.now()))
# print("done")


# table2 = pq.read_table('example.parquet')
# print(table2.to_pandas())
# d1 = {"header": {"uuid": str(uuid.uuid4()), "type":"store-transaction", "created":1461024358442,"modified":1461024358442,"alias":"245^20160408^63^8794","transient":False,"sku":sku}}
# df2 = pd.io.json.json_normalize(d1)
# new_df2 = pd.DataFrame(df2)
# with pd.option_context('display.max_rows', None, 'display.max_columns', None):print(new_df1)
