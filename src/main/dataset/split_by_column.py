import csv
import time
import random
from datetime import datetime, timedelta
from typing import Optional

"""
below code lines can split a file into multiple small files
by 'type' column column
"""
def partition_by_type(input_file: str, num_files: int, seed: Optional[int] = None) -> None:
    rng = random.Random(seed)
    in_path = input_file + '.csv'
    with open(in_path, newline='') as infile:
        reader = csv.reader(infile)
        header = next(reader)

        try:
            type_idx = next(i for i, h in enumerate(header) if h.lower() == 'type')
        except StopIteration:
            raise RuntimeError("cannot find column: 'type'")

        # open file and write header
        outs = [open(f'{input_file}_{i}.csv', 'w', newline='') for i in range(num_files)]
        writers = [csv.writer(f) for f in outs]
        for w in writers:
            w.writerow(header)

        try:
            type_map = {}
            for row in reader:
                t = row[type_idx].lower()
                if t not in type_map:
                    type_map[t] = rng.randrange(num_files)
                writers[type_map[t]].writerow(row)
        finally:
            for f in outs:
                f.close()

"""
below code lines can split a file into multiple small files
by 'eventTime' column
"""
def partition_by_event_time(input_file: str, num_files: int, window_seconds: int, start_time: Optional[int] = None, seed: Optional[int] = None) -> None:
    rng = random.Random(seed)
    in_path = input_file + '.csv'

    # first pass: locate header and find eventTime column index and minimal timestamp if start_time is None
    with open(in_path, newline='') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        try:
            time_idx = next(i for i, h in enumerate(header) if h.lower() == 'eventtime')
        except StopIteration:
            raise RuntimeError("cannot find column: 'eventTime'")

    # second pass: write rows into window->file mapping
    outs = [open(f'{input_file}_{i}.csv', 'w', newline='') for i in range(num_files)]
    writers = [csv.writer(f) for f in outs]
    for w in writers:
        w.writerow(header)

    try:
        window_map = {}
        with open(in_path, newline='') as infile:
            reader = csv.reader(infile)
            next(reader)  # skip header
            for row in reader:
                if len(row) <= time_idx:
                    continue
                raw = row[time_idx]
                if raw == '':
                    continue
                try:
                    ts = int(float(raw))
                except ValueError:
                    continue
                if input_file == 'citibike' or input_file.find('synthetic') != -1:
                    # [ms]
                    ts = ts // 1000
                elif input_file == 'cluster':
                    # [ns]
                    ts = ts // 1_000_000

                # compute window index
                window_idx = ts // window_seconds
                key = int(window_idx)
                if key not in window_map:
                    window_map[key] = rng.randrange(num_files)
                writers[window_map[key]].writerow(row)
    finally:
        for f in outs:
            f.close()


# please modify related setting
if __name__ == '__main__':
    input_file = 'crimes'    # without .csv
    num_files = 3
    seed = 42

    start_time = time.perf_counter()

    # --> partition by type
    partition_by_type(input_file, num_files, seed=seed)

    # --> partition by time window, 30 minutes = 1800 seconds
    # partition_by_event_time(input_file, num_files, window_seconds=1800, start_time=None, seed=seed)

    end_time = time.perf_counter()
    elapsed_time = timedelta(seconds=(end_time - start_time))
    print(f"runtime: {elapsed_time}")

    print("finish partition...")
