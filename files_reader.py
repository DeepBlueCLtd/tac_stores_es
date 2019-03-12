import os
from os import walk

# path to the destination data directory
dest_dir = "res_files/"

# files in the data folder
all_filenames_data = []
dsf_files = []
rep_files = []

# reqursive function for handling folder of data
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
                    if filename.endswith("."+f_type):
                        globals()[f_type + "_files"].append(path + filename)

        if dirnames:
            # go through directories
            for dirname in dirnames:
                # recursive calling the function
                recursive_read(path + dirname + "/", possible_types)
        break


def get_files(path, possible_types=[]):
    recursive_read(path, possible_types)

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # save all filenames
    file = open(dest_dir + "list_of_all_filenames.txt", "w")
    for filepath in all_filenames_data:
        file.write(filepath + '\n')
    file.close()

    # save rep files
    # file = open(dest_dir + "list_rep_filenames.txt", "w")
    # for filepath in rep_files:
    #     file.write(filepath + '\n')
    # file.close()
    #
    # # save dsf files
    # file = open("res_files/list_dsf_filenames.txt", "w")
    # for filepath in dsf_files:
    #     file.write(filepath + '\n')
    # file.close()

    return (globals()[p_type + "_files"] for p_type in possible_types)
