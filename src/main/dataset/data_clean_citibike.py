"""
clean citibike dataset (dataset description please see https://citibikenyc.com/system-data)
download ulr: https://s3.amazonaws.com/tripdata/2023-citibike-tripdata.zip
------------------------------------------------------------------------------------------------------
rideable_type: {'electric_bike': 17605824, 'classic_bike': 17501206}
member_casual: {'member': 28,513,791, 'casual': 6,593,239}
------------------------------------------------------------------------------------------------------
|Duration time  |0-5min |5-10min |10-15min|15-20min|20-25m |25-30m |30-60m |1-2hour|2-6hour|>6hour|
|Number of event|7944838|10738540|6335179 |3605527 |2174998|1380798|2383886|405436 |76828  |60676 |
------------------------------------------------------------------------------------------------------
Duration    Name          Number    Type
[0-5m)      transient     7.9M      {A,B,C,D}
[5-10m)     short        10.7M      {E,F,G,H}
[10-20m)    middle        7.9M      {I,J,K,L}
[20-30m)    long          2.5M      {M,N,O,P}
[30m-inf]   continual     2.8M      {Q,R,S,T}
------------------------------------------------------------------------------------------------------
discard code:
# 0, 5 min, 10, 15, 20, 25, 30, 60, 120,
bins = [0, 300, 600, 900, 1200, 1500, 1800, 3600, 7200, 19600, 1000000000]
labels = ['0-5m', '5-10m', '10-15m', '15-20m', '20-25m', '25-30m', '30-60m', '1-2h', '2-6h', '>6h']
data['ranges'] = pd.cut(data['duration_time'], bins=bins, labels=labels, right=True)
count_per_range = data['ranges'].value_counts()
for range_label, count in count_per_range.items():
        total_counts[range_label] += count
------------------------------------------------------------------------------------------------------
"""

from collections import defaultdict
import pandas as pd
import numpy as np

# 'citibike_mini.csv', 'citibike_mini.csv'
file_names = [
'202301-citibike-tripdata_1.csv','202301-citibike-tripdata_2.csv','202302-citibike-tripdata_1.csv','202302-citibike-tripdata_2.csv',
'202303-citibike-tripdata_1.csv','202303-citibike-tripdata_2.csv','202303-citibike-tripdata_3.csv','202304-citibike-tripdata_1.csv',
'202304-citibike-tripdata_2.csv','202304-citibike-tripdata_3.csv','202305-citibike-tripdata_1.csv','202305-citibike-tripdata_2.csv',
'202305-citibike-tripdata_3.csv','202305-citibike-tripdata_4.csv','202306-citibike-tripdata_1.csv','202306-citibike-tripdata_2.csv',
'202306-citibike-tripdata_3.csv','202306-citibike-tripdata_4.csv','202307-citibike-tripdata_1.csv','202307-citibike-tripdata_2.csv',
'202307-citibike-tripdata_3.csv','202307-citibike-tripdata_4.csv','202308-citibike-tripdata_1.csv','202308-citibike-tripdata_2.csv',
'202308-citibike-tripdata_3.csv','202308-citibike-tripdata_4.csv','202309-citibike-tripdata_1.csv','202309-citibike-tripdata_2.csv',
'202309-citibike-tripdata_3.csv','202309-citibike-tripdata_4.csv','202310-citibike-tripdata_1.csv','202310-citibike-tripdata_2.csv',
'202310-citibike-tripdata_3.csv','202310-citibike-tripdata_4.csv','202311-citibike-tripdata_1.csv','202311-citibike-tripdata_2.csv',
'202311-citibike-tripdata_3.csv','202312-citibike-tripdata_1.csv','202312-citibike-tripdata_2.csv','202312-citibike-tripdata_3.csv'
]

# total_counts = defaultdict(int)
sum_count_ss_id = defaultdict(int)

def hex_to_signed_long(hex_str):
    value = int(hex_str, 16)
    if value >= 2**63:
        value -= 2**64
    return value

for i in range(len(file_names)):
    file_name = file_names[i]
    # load data
    data = pd.read_csv(file_name)

    data['start_station_id'].astype(str)
    data['end_station_id'].astype(str)

    data['duration_time'] = (pd.to_datetime(data['ended_at']) - pd.to_datetime(data['started_at'])).dt.total_seconds()
    conditions = [
        (data['duration_time'] < 300) & (data['rideable_type'] == 'classic_bike') & (data['member_casual'] == 'member'),
        (data['duration_time'] < 300) & (data['rideable_type'] == 'classic_bike') & (data['member_casual'] == 'casual'),
        (data['duration_time'] < 300) & (data['rideable_type'] == 'electric_bike') & (data['member_casual'] == 'member'),
        (data['duration_time'] < 300) & (data['rideable_type'] == 'electric_bike') & (data['member_casual'] == 'casual'),
        (data['duration_time'] < 600) & (data['rideable_type'] == 'classic_bike') & (data['member_casual'] == 'member'),
        (data['duration_time'] < 600) & (data['rideable_type'] == 'classic_bike') & (data['member_casual'] == 'casual'),
        (data['duration_time'] < 600) & (data['rideable_type'] == 'electric_bike') & (data['member_casual'] == 'member'),
        (data['duration_time'] < 600) & (data['rideable_type'] == 'electric_bike') & (data['member_casual'] == 'casual'),
        (data['duration_time'] < 1200) & (data['rideable_type'] == 'classic_bike') & (data['member_casual'] == 'member'),
        (data['duration_time'] < 1200) & (data['rideable_type'] == 'classic_bike') & (data['member_casual'] == 'casual'),
        (data['duration_time'] < 1200) & (data['rideable_type'] == 'electric_bike') & (data['member_casual'] == 'member'),
        (data['duration_time'] < 1200) & (data['rideable_type'] == 'electric_bike') & (data['member_casual'] == 'casual'),
        (data['duration_time'] < 1800) & (data['rideable_type'] == 'classic_bike') & (data['member_casual'] == 'member'),
        (data['duration_time'] < 1800) & (data['rideable_type'] == 'classic_bike') & (data['member_casual'] == 'casual'),
        (data['duration_time'] < 1800) & (data['rideable_type'] == 'electric_bike') & (data['member_casual'] == 'member'),
        (data['duration_time'] < 1800) & (data['rideable_type'] == 'electric_bike') & (data['member_casual'] == 'casual'),
        (data['duration_time'] >= 1800) & (data['rideable_type'] == 'classic_bike') & (data['member_casual'] == 'member'),
        (data['duration_time'] >= 1800) & (data['rideable_type'] == 'classic_bike') & (data['member_casual'] == 'casual'),
        (data['duration_time'] >= 1800) & (data['rideable_type'] == 'electric_bike') & (data['member_casual'] == 'member'),
        (data['duration_time'] >= 1800) & (data['rideable_type'] == 'electric_bike') & (data['member_casual'] == 'casual')
    ]

    choices = [
        'A', 'B', 'C', 'D',
        'E', 'F', 'G', 'H',
        'I', 'J', 'K', 'L',
        'M', 'N', 'O', 'P',
        'Q', 'R', 'S', 'T'
    ]
    data['type'] = np.select(conditions, choices, default='X')

    # defualt is ns, then we divide 10^6, get ms
    data['eventTime'] = pd.to_datetime(data['started_at']).astype('int64') // 10 ** 6

    # delete columns
    data.drop(columns=['rideable_type', 'started_at', 'ended_at', 'start_station_name', 'end_station_name', 'member_casual', 'duration_time'], inplace=True)

    # delete the rows with null
    data['start_station_id'] = pd.to_numeric(data['start_station_id'], errors='coerce')
    data['end_station_id'] = pd.to_numeric(data['end_station_id'], errors='coerce')
    data.dropna(inplace=True)

    df['ride_id'] = df['ride_id'].apply(hex_to_signed_long)
    df['start_station_id'] = (df['start_station_id'] * 100).astype(int)
    df['end_station_id'] = (df['end_station_id'] * 100).astype(int)

    # generate new order
    new_order = ['type', 'ride_id', 'start_station_id', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'eventTime']
    data = data[new_order]
    # print(data.head())
    data.to_csv('citibike' + str(i) + '.csv', index=False)

print('finish...')

# please run below shell scripts to merge all small files
"""
{
    cat citibike0.csv
    for i in {1..39}; do
        sed '1d' citibike${i}.csv
    done
} > merged_file.csv
"""