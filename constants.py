import yaml
import os

# Load configuration from YAML file
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Paths
INPUT_FOLDER = config['paths']['input_folder']
OUTPUT_FOLDER = config['paths']['output_folder']

DICTIONARY_FOLDER = config['paths']['dictionary_folder']
DICTIONARY_FILE= 'updated_mapping.json'
DICTIONARY_PATH= os.path.join(DICTIONARY_FOLDER, DICTIONARY_FILE)

# Files
SOURCE_DATASET = config['files']['source_dataset']
TARGET_DATASET = config['files']['target_dataset']
SOURCE_DATASET_DICTIONARY = config['files']['source_dataset_dictionary']
FILTERED_MAPPED_TRANSFORMED_CSV = config['files']['filtered_mapped_transformed_csv']
ENTITY_RESOLUTION_RESULTS_CSV = config['files']['entity_resolution_results_csv']

# Thresholds
DROP_NA_THRESHOLD = config['thresholds']['drop_na_threshold']

# Columns
ADDRESS_COLUMN = config['columns']['address_column']
