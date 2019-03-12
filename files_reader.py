import os
from os import walk

# path to the data directory
SOURCE_PATH = "data/"
dest_dir = "res_files/"

# files in the data folder
all_filenames_data = []
dsf_files = []
rep_files = []

# file types allowed for handling
file_types = [".dsf", ".rep"]

# reqursive function for handling folder of data
def recursive_read(path):
    # print(path)
    for (dirpath, dirnames, filenames) in walk(path):
        if filenames:
            for filename in filenames:
                all_filenames_data.append(path + filename)

                if filename.endswith(".dsf"):
                    dsf_files.append(path+filename)

                if filename.endswith(".rep"):
                    rep_files.append(path+filename)

        if dirnames:
            for dirname in dirnames:
                recursive_read(path + dirname + "/")
        break


def input_function():
    path = SOURCE_PATH
    recursive_read(path)

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # save all filenames
    file = open(dest_dir + "list_of_all_filenames.txt", "w")
    for filepath in all_filenames_data:
        file.write(filepath + '\n')
    file.close()

    # save rep files
    file = open(dest_dir + "list_rep_filenames.txt", "w")
    for filepath in rep_files:
        file.write(filepath + '\n')
    file.close()

    # save dsf files
    file = open("res_files/list_dsf_filenames.txt", "w")
    for filepath in dsf_files:
        file.write(filepath + '\n')
    file.close()

# run script
input_function()