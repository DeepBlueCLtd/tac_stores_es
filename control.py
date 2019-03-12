import files_reader
import documents_preparation
import es_import

# path to the data directory
SOURCE_PATH = "data/"

files = files_reader.get_files(SOURCE_PATH, ["dsf", "rep"])
states, contacts = documents_preparation.get_documents_from(files)
es_import.store_data(states, contacts)