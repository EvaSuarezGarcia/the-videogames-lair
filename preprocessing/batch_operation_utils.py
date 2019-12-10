import csv
from os import fsync
from os.path import isfile


def read_next_batch_data(next_batch_file, finished_mark="Finished"):
    info = []
    if isfile(next_batch_file):
        with open(next_batch_file) as next_file:
            csv_reader = csv.reader(next_file, delimiter=",")
            info = next(csv_reader)
            if info[0] == finished_mark:
                info = None
    return info


def write_info(info_file_path, open_mode, info_rows):
    with open(info_file_path, open_mode) as info_file:
        csv_writer = csv.writer(info_file, delimiter=",")
        csv_writer.writerows(info_rows)
        info_file.flush()
        fsync(info_file.fileno())
