import pandas as pd

"""
part-00000-of-00500.csv

timestamp is (us)
"""

column_names=['timestamp', 'missing_info', 'job_ID', 'task_index_within_the_job',
              'machine_ID', 'event_type', 'user_name', 'scheduling_class', 'priority',
              'resource_request_for_CPU_cores', 'resource_request_for_RAM',
              'resource_request_for_local_disk_space', 'different_machine_constraint']

start_id = 0
end_id = 500
for i in range(start_id, end_id):
    filename = f"part-{i:05d}-of-00500.csv"
    data = pd.read_csv(filename, header=None, names=column_names)

    # drop columns
    data.drop(columns=['missing_info', 'machine_ID', 'user_name', 'different_machine_constraint'], inplace=True)

    # drop null value
    data.dropna(inplace=True)

    # generate new order
    new_order = ['event_type', 'job_ID', 'task_index_within_the_job', 'scheduling_class',
                 'priority', 'resource_request_for_CPU_cores', 'resource_request_for_RAM',
                 'resource_request_for_local_disk_space', 'timestamp']
    data = data[new_order]
    data.to_csv('cluster' + str(i) + '.csv', index=False)

print('finish...')

# please run below shell scripts to merge all small files
"""
#!/bin/bash

{
    cat cluster0.csv
    for i in {1..499}; do
        sed '1d' cluster${i}.csv
    done
} > merged_file.csv

for i in {0..500}; do
    rm -rf cluster${i}.csv
done
echo 'finish'
"""