import pandas as pd
from datetime import datetime
from feast import FeatureStore

data = pd.read_parquet("data/driver_stats.parquet")

new_data = data.copy()
new_data['avg_daily_trips'] = new_data['avg_daily_trips'] + 1000
new_data['event_timestamp'] = new_data['event_timestamp']
new_data['created'] = pd.Timestamp.now()

merged_df = pd.concat([data, new_data]).reset_index(drop=True)
merged_df.to_parquet("./data/driver_stats.parquet")

store = FeatureStore(".")
entity_df = pd.DataFrame.from_dict(
    {
        # entity's join key -> entity values
        "driver_id": [1001, 1002, 1003],
        # "event_timestamp" (reserved key) -> timestamps
        "event_timestamp": [
            datetime(2021, 4, 12, 16, 40, 26),
            datetime(2021, 4, 12, 16, 40, 26),
            datetime(2021, 4, 12, 16, 40, 26),
        ]
    }
)
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips"
    ],
).to_df()
print(training_df)  # it is right, result has (plus+1000) datas

new_data = data.copy()
new_data['avg_daily_trips'] = new_data['avg_daily_trips'] + 10000
new_data['event_timestamp'] = new_data['event_timestamp']
new_data['created'] = pd.Timestamp.now()
# this "merged_df" which is merged by pd.concat is above one.
merged_df = pd.concat([merged_df, new_data]).reset_index(drop=True)
merged_df.to_parquet("./data/driver_stats.parquet")

store = FeatureStore(".")
entity_df = pd.DataFrame.from_dict(
    {
        # entity's join key -> entity values
        "driver_id": [1001, 1002, 1003],
        # "event_timestamp" (reserved key) -> timestamps
        "event_timestamp": [
            datetime(2021, 4, 12, 16, 40, 26),
            datetime(2021, 4, 12, 16, 40, 26),
            datetime(2021, 4, 12, 16, 40, 26),
        ]
    }
)
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips"
    ],
).to_df()
print(training_df)