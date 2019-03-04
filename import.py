import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

file_paths_array = ["data/track_one.rep", "data/track_two.rep", "data/sen_frig_sensor.dsf"]
es = Elasticsearch()

elastic_data = []
col_names = ["date/time", "yymmdd", "hummus.SSS", "platform", "colour", "position",
             "course", "speed", "depth"]


def format_position(info_arr):
    try:
        # degrees + minutes / 60 + seconds / (60 * 60)
        i_lat = int(float(info_arr[4])) + (int(float(info_arr[5])) / 60) + (int(float(info_arr[6])) / (60 * 60))
        i_lon = int(float(info_arr[8])) + (int(float(info_arr[9])) / 60) + (int(float(info_arr[10])) / (60 * 60))

        if info_arr[7] == 'S':
            i_lat = -1 * i_lat

        if info_arr[11] == 'W':
            i_lon = -1 * i_lon
    except Exception:
        i_lat = None
        i_lon = None

    return i_lat, i_lon


def run_files_import(file_paths):
    for path in file_paths:

        with open(path, "r") as f:
            measurements = [line.strip() for line in f.readlines()]

            for ind, item in enumerate(measurements):

                # @TODO omit the head of table, should be depended on the string not on index number
                if ind == 0:
                    continue

                # single object for each record
                elastic_entry = {}
                # splitting the line off
                info = item.split()

                if not info:
                    continue

                if "SENSOR" in info[0]:
                    tuple_tmp = (
                        info[1], info[2], info[3], info[4], info[5], info[6], info[7], info[8], info[9], info[10])
                    new_item = ' '.join(tuple_tmp)
                    info = new_item.split()

                # time field
                date_str = info[0] + " " + info[1]
                date_time_obj = datetime.datetime.strptime(date_str, '%y%m%d %H%M%S.%f')
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

                # position field
                lat, lon = format_position(info)
                elastic_entry["location"] = {"lat": lat, "lon": lon}

                # custom type field
                elastic_entry["meas_type"] = "default"

                elastic_data.append(elastic_entry)


def transfer_data(input_data=[]):
    for entry in input_data:
        # print(entry)
        yield {
            '_op_type': 'index',
            "_index": "measurements",
            "_type": "geo_data",
            "_source": entry
        }


doc = {
    'author': 'deep blue',
    'text': 'Geo data 2 Elasticsearch',
    'timestamp': datetime.datetime.now(),
}

es.indices.delete(index='measurements', ignore=[400, 404])
es.index(index="measurements", doc_type='geo_data', id=1, body=doc)

print("Preparing data")
run_files_import(file_paths_array)
print("Transferring the data")
transfer_data(elastic_data)
helpers.bulk(es, transfer_data(elastic_data), chunk_size=1000)
