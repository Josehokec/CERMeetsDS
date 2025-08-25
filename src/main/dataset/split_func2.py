"""
below code lines can uniformly split a file into multiple small file
please modify input_file and num_files
"""

input_file = 'synthetic50M'
num_files = 2

with open(input_file + '.csv', 'r') as infile:
    output_files = [open(f'{input_file}_{i}.csv', 'a') for i in range(num_files)]
    try:
        for i, line in enumerate(infile):
            # header line will store in all small files
            if i == 0:
                for output_file in output_files:
                    output_file.write(line)
            else:
                file_index = i % num_files
                output_files[file_index].write(line)
    finally:
        for f in output_files:
            f.close()

print("split file successfully")