import pandas as pd
import uuid
import random

d = {"header": {"uuid": str(uuid.uuid4()), "type": "store-transaction", "created": 1461024358442,
                "modified": 1461024358442, "alias": "245^20160408^63^8794", "transient": False,
                "sku": random.randint(1, 1000001)}}
df1 = pd.io.json.json_normalize(d)
df1.columns = df1.columns.map(lambda x: x.split(".")[-1])
new_df1 = pd.DataFrame(df1)
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(df1)