import pandas as pd
from datetime import datetime ,timedelta
import os


def apply_retention_policy(config, days=30):
    '''keep only last N days' data in silver'''

    s_path = config['data']['silver_path']
    file_path = os.path.join(s_path, "exchange_rates.parquet")
    if not os.path.exists(file_path):
        return

    df = pd.read_parquet(file_path)
    cutoff = datetime.today() - timedelta(days=days)
    df_new = df[df['rate_date']>=cutoff]
    df_new.to_parquet(file_path,index=False)