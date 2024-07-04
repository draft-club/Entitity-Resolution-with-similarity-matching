import pandas as pd
import os
import logging
from calculate import EntityResolution
from describe import DataFrameDescriber, process_csv_files
from mapping import DataFrameMapper
from normalize import AddressNormalization
from prepare import DataFrameProcessor
from utils import load_and_concatenate_data
import yaml

# Import constants by name
from constants import (
    INPUT_FOLDER,
    OUTPUT_FOLDER,
    DICTIONARY_PATH,
    SOURCE_DATASET,
    TARGET_DATASET,
    SOURCE_DATASET_DICTIONARY,
    FILTERED_MAPPED_TRANSFORMED_CSV,
    ENTITY_RESOLUTION_RESULTS_CSV,
    DROP_NA_THRESHOLD,
    ADDRESS_COLUMN
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():


    # Load configuration from YAML file
    with open("config.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)
    try:
        # Step 1: Load data and dictionaries
        combined_df = load_and_concatenate_data(config)
        print(len(combined_df))
        # Sort by 'nom' column
        combined_df = combined_df.sort_values(by='authors')
        # Take only the first 10,000 rows
        combined_df = combined_df.head(10000)
        print(len(combined_df))
        exit(1)


        logging.info("Data loaded successfully.")

        # Step 2: Apply mapping
        """
        mapper = DataFrameMapper(df, dict_path)
        mapping_dict = mapper.load_mapping_dict()
        mapper.apply_mapping()
        mapped_df = mapper.df
        logging.info("Mapping applied successfully.")"""

        mapped_df = combined_df  # Placeholder for tests

        # Step 3: Describe data
        describer = DataFrameDescriber(mapped_df)
        describer.get_info()
        describer.get_description()
        describer.get_missing_values()
        describer.get_missing_values_percentage_by_column()
        describer.get_missing_values_percentage_by_row()
        describer.get_unique_values()

        # Export description to Excel
        description_output_path = os.path.join(OUTPUT_FOLDER, 'data_description.xlsx')
        # describer.export_to_excel(description_output_path)
        logging.info("Data description exported successfully.")

        # Step 4: Prepare data
        processor = DataFrameProcessor(mapped_df)
        processor.drop_columns_with_high_nas(threshold=DROP_NA_THRESHOLD)

        # Filter and save processed DataFrame
        """filtered_output_path = os.path.join(OUTPUT_FOLDER, FILTERED_MAPPED_TRANSFORMED_CSV)
        processor.filter_and_save_df(columns_dict=mapping_dict, output_path=filtered_output_path)"""
        logging.info("Data prepared and saved successfully.")

        # Step 5: Normalize addresses
        if ADDRESS_COLUMN in processor.df.columns:
            address_normalizer = AddressNormalization(processor.df)
            address_normalizer.normalize_addresses(address_column=ADDRESS_COLUMN)

            # Export normalized data
            normalized_output_path = os.path.join(OUTPUT_FOLDER, 'normalized_addresses.csv')
            address_normalizer.export_results(normalized_output_path)
            logging.info("Addresses normalized and exported successfully.")


        # Step 6: Calculate similarities
        resolver = EntityResolution(processor.df)
        resolver.sort_and_export_results(output_path=os.path.join(OUTPUT_FOLDER, ENTITY_RESOLUTION_RESULTS_CSV))
        logging.info("Entity resolution completed and results exported successfully.")

        # Step 7: Generate synthetic tables
        process_csv_files(INPUT_FOLDER)
        logging.info("Synthetic tables generated successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
