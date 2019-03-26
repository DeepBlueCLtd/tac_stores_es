import os
from os import walk

# files in the data folder
all_filenames_data = []


# recursive function for handling folder of data
def recursive_read(path, possible_types=[]):
    # print(path)
    # go through the path
    for (dirpath, dirnames, filenames) in walk(path):
        if filenames:
            # handling files in directory
            for filename in filenames:
                all_filenames_data.append(path + filename)
                # checking file types
                for f_type in possible_types:

                    if filename.endswith("." + f_type):
                        globals()[f_type + "_files"].append(path + filename)


        if dirnames:
            # go through directories
            for dirname in dirnames:
                # recursive calling the function
                recursive_read(path + dirname + "/", possible_types)
        break


def get_files(path, possible_types=[]):
    for p_type in possible_types:
        globals()[p_type + "_files"] = []

    recursive_read(path, possible_types)

    return {p_type: globals()[p_type + "_files"] for p_type in possible_types}
