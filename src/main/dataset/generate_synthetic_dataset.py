import time
import numpy as np
import random
from datetime import datetime, timedelta

"""
[code explanation]:
- lambda_rate: event occurrence frequency
If the number of occurrences of a random event per unit time follows a Poisson distribution,
then the time interval between consecutive occurrences of the event will follow an exponential distribution.

event schema{
    char[4] type [Zipf(n=20, skew=1.2)],       # T_00~T_19
    int a1(Uniform),
    char[4] a2 [Zipf(n=50, skew=0.255)],       # ATTR2_00 ~ ATTR2_49
    char[4] a3 [Zipf(n=50, skew=0.7)],         # ATTR3_00_PADDING ~ ATTR3_49_PADDING
    double timestamp
}

[0.3498, 0.1523, 0.0936, 0.0663, 0.0507,
0.0407, 0.0339, 0.0288, 0.0250, 0.0221,
0.0197, 0.0177, 0.0161, 0.0147, 0.0136,
0.0126, 0.0117, 0.0109, 0.0102, 0.0096]


----------------------------
|    n    |    time cost   |
----------------------------
|   50M   | 0:03:04.572501 |
|  100M   | 0:05:53.147719 |
"""

# set random seed
np.random.seed(7)
rng = random.Random()
rng.seed(7)

# 1741944117 --> '2025-03-14 17:21:57'
def generate_timestamps(lambda_rate=0.1, num_events=1000, start_time=1741944117):
    intervals = np.random.exponential(scale=1/lambda_rate, size=num_events)
    timestamps = [0.0 for i in range(num_events + 1)]
    timestamps[0] = start_time
    for idx in range(num_events):
        distance = intervals[idx]
        timestamps[idx + 1] = timestamps[idx] + distance
    unix_timestamps = [0 for i in range(num_events)]
    for idx in range(num_events):
        unix_timestamps[idx] = timestamps[idx + 1]
    return unix_timestamps, unix_timestamps[-1]

def getZipfCDFs(num_type=100, skew=0.255):
    # note that c = \sum_{i=1}^{n} \cdot \frac{1}{i ^ skew}
    c = 0
    for i in range(1, num_type + 1):
        c = c + (1 / i) ** skew
    pro = [0.0 for i in range(num_type)]
    for i in range(1, num_type + 1):
        pro[i-1] = 1 / ((i ** skew) * c)
    # print('pro', pro)
    # cdf: Cumulative Distribution Function
    cdf = [0.0 for _ in range(num_type)]
    cdf[0] = pro[0]
    for i in range(1, num_type - 1):
        cdf[i] = cdf[i - 1] + pro[i]
    cdf[num_type - 1] = 1
    return cdf

def get_zipf_data(num_event=1000, num_type=100, skew=0.255, prefix='T_', suffix=''):
    cdf = getZipfCDFs(num_type, skew)
    # print('cdf: ', cdf)
    type_names = []
    for i in range(num_event):
        r = rng.random()
        # binary search
        left = 0
        right = num_type - 1
        mid = (left + right) >> 1
        while left <= right:
            # print('mid: ', mid)
            if r <= cdf[mid]:
                if mid == 0 or r > cdf[mid - 1]:
                    break
                else:
                    right = mid - 1
                    mid = (left + right) >> 1
            else:
                if mid == num_type - 1:
                    break
                else:
                    left = mid + 1
                    mid = (left + right) >> 1
        type_names.append(prefix + f"{mid:02}" + suffix)
    return type_names

def get_uniform_data(min=0, max=1000, num_event=1000):
    # if you want to get double
    # uniform_data = [random.uniform(0, 1000) for _ in range(100)]
    return [rng.randint(min, max) for _ in range(num_event)]

def get_gauss_data(mu=0, sigma=100, num_event=1000):
    return [round(rng.gauss(mu, sigma), 8) for _ in range(num_event)]


# getZipfCDFs(num_type=20, skew=1.2)
# getZipfCDFs(num_type=50, skew=0.7)
# exit()

# 50M -> 50_000_000
# 500M -> 500_000_000
DEFINE_NUM =50_000_000
# batch write
BATCH_LEN = 1_000_000

def generate_dataset(filename):
    last_time= 2041927911       # old value: 1741944117, 1841938715, 1941933313, 2041927911
    with open(filename, 'w') as file:
        # 32bytes = char[4], int, double, char[4], char[4], long
        file.write('type,a1,a2,a3,ts[ms]\n')
        num_batch = int(DEFINE_NUM / BATCH_LEN)
        for batch_id in range(num_batch):
            type_list = get_zipf_data(num_event=BATCH_LEN, num_type=20, skew=1.2)
            # id_list = [i for i in range(batch_id * BATCH_LEN + 1, (batch_id + 1) * BATCH_LEN + 1)]
            a1_list = get_uniform_data(num_event=BATCH_LEN)
            a2_list = get_zipf_data(num_event=BATCH_LEN, num_type=50, skew=0.255, prefix='ATTR2_')
            a3_list = get_zipf_data(num_event=BATCH_LEN, num_type=50, skew=0.7, prefix='ATTR3_', suffix='_PADDING')
            ts_list, last_time = generate_timestamps(lambda_rate=1, num_events=BATCH_LEN, start_time=last_time)

            # please note that we randomly shuffle data to ensure out-of-order
            rng.shuffle(ts_list)

            write_content = ''
            for idx in range(BATCH_LEN):
                # {ts_list[idx] :.3f}   {id_list[idx]}
                # content = f"{type_list[idx]},{a1_list[idx]},{a2_list[idx]},{a3_list[idx]},{int (ts_list[idx] * 1000)}\n"

                content = f"{int (ts_list[idx] * 1000)}\n"

                write_content = write_content + content
                # write_content = ''.join([write_content, content])
            file.write(write_content)
            # file.flush()
        print('last_time: ', last_time)

start_time = time.perf_counter()
generate_dataset('synthetic50M.csv')
end_time = time.perf_counter()
elapsed_time = timedelta(seconds=(end_time - start_time))
print(f"rumtime: {elapsed_time}")

