from os import walk

# path to the data directory
path = "data/"
# common array of all files in the data folder
filenames_data = []


# reqursive function for handling folder of data
def recursive_read(path):
    # print(path)
    for (dirpath, dirnames, filenames) in walk(path):
        # print(dirnames)
        # print(filenames)
        # print(dirpath)
        if filenames:
            for filename in filenames:
                filenames_data.append(path + filename)

        if dirnames:
            for dirname in dirnames:
                recursive_read(path + dirname + "/")
        break


recursive_read(path)

print(filenames_data)

file = open("list_of_filenames.txt","w")
for filepath in filenames_data:
    file.write(filepath+'\n')
file.close()
