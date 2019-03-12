# ["date/time", "yymmdd", "hummus.SSS", "platform", "colour", "position", "course", "speed", "depth"]

import datetime
import re

PATHS_OF_FILES = "res_files/list_of_all_filenames.txt"

elastic_data_contacts = []
elastic_data_states = []

error_log_file = open("error_log.txt", "w+")


def read_files_list(file_types):
    arr = []
    for file_type in file_types:
        arr = arr + file_types[file_type]
    return arr


def format_position(info_arr):
    # degrees + minutes / 60 + seconds / (60 * 60)
    i_lat = int(float(info_arr[4])) + (int(float(info_arr[5])) / 60) + (int(float(info_arr[6])) / (60 * 60))
    i_lon = int(float(info_arr[8])) + (int(float(info_arr[9])) / 60) + (int(float(info_arr[10])) / (60 * 60))

    if info_arr[7] == 'S':
        i_lat = -1 * i_lat

    if info_arr[11] == 'W':
        i_lon = -1 * i_lon

    return i_lat, i_lon


def date_time_format(date_str):
    x = re.search("\d+\.+?\d*", date_str)
    if x is None:
        short_d = re.match("(19|20)\d{2}", date_str)
        if short_d and short_d.group():
            date_time_obj = datetime.datetime.strptime(date_str, '%Y%m%d %H%M%S')
        else:
            date_time_obj = datetime.datetime.strptime(date_str, '%y%m%d %H%M%S')
    else:
        z = re.match("(19|20)\d{2}", date_str)
        if z and z.group():
            date_time_obj = datetime.datetime.strptime(date_str, '%Y%m%d %H%M%S.%f')
        else:
            date_time_obj = datetime.datetime.strptime(date_str, '%y%m%d %H%M%S.%f')

    return date_time_obj


def run_files_import(file_paths):
    for path in file_paths:
        try:
            with open(path, "r") as f:
                measurements = [line.strip() for line in f.readlines()]

                for ind, item in enumerate(measurements):
                    # single object for each record
                    elastic_entry = {}
                    # splitting the line off
                    info = item.split()

                    if not info:
                        continue

                    # @TODO this is not perfect solution it works with current data
                    if ';;' in info[0] or ';TIMETEXT:' in info[0] or ';PERIODTEXT:' in info[0] or ';TEXT:' in info[0]:
                        continue

                    # Solution for task: Refactoring python scripts #4
                    es_index = ''
                    if ';SENSOR:' in info[0] or ';SENSOR2:' in info[0]:
                        es_index = 'contacts'
                    elif int(info[0]) and (len(info[0]) == 6 or len(info[0]) == 8):
                        es_index = 'states'

                    # Checking the type of file and using of needed logic
                    if "SENSOR" in info[0]:
                        tuple_tmp = (
                            info[1], info[2], info[3], info[4], info[5], info[6], info[7], info[8], info[9], info[10])
                        new_item = ' '.join(tuple_tmp)
                        info = new_item.split()
                        # position field
                        elastic_entry["location"] = {"lat": None, "lon": None}
                    else:
                        # position field
                        lat, lon = format_position(info)
                        elastic_entry["location"] = {"lat": lat, "lon": lon}

                    # time field
                    date_str = info[0] + " " + info[1]
                    date_time_obj = date_time_format(date_str)
                    elastic_entry["time"] = date_time_obj

                    # platform field
                    elastic_entry["platform"] = info[2]

                    # color field
                    elastic_entry["color"] = info[3]

                    # course field
                    if 12 in info:
                        elastic_entry["course"] = info[12]
                    else:
                        elastic_entry["course"] = None

                    # speed field
                    if 13 in info:
                        elastic_entry["speed"] = info[13]
                    else:
                        elastic_entry["speed"] = None

                    # depth field
                    if 14 in info:
                        elastic_entry["depth"] = info[14]
                    else:
                        elastic_entry["depth"] = None

                    # serial field
                    elastic_entry["serial"] = "EX_ALPHA"

                    # sensor field
                    elastic_entry["sensor"] = "GPS"

                    # source field
                    elastic_entry["source"] = "CD_123"

                    # privacy field
                    elastic_entry["privacy"] = "public"

                    # custom type field
                    elastic_entry["meas_type"] = "default"

                    # es index name
                    elastic_entry['es_index'] = es_index

                    if es_index == "contacts":
                        elastic_data_contacts.append(elastic_entry)
                    elif es_index == "states":
                        elastic_data_states.append(elastic_entry)

        except ValueError as e:
            print(ValueError)
            import datetime
            now = datetime.datetime.now()
            error_log_file.write('File path: ' + path + '; Error: ' + str(e) + '; Time: ' + now.isoformat() + '\n')


def get_documents_from(files):
    print("Reading list of files")
    arr = read_files_list(files)
    print("Preparing data")
    run_files_import(arr)

    return elastic_data_states, elastic_data_contacts
