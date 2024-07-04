import pandas as pd
import chardet
import os


class CSVReader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.encoding = self.detect_encoding()

    def detect_encoding(self):
        with open(self.file_path, 'rb') as file:
            result = chardet.detect(file.read())
        return result['encoding']

    def read_csv(self):
        return pd.read_csv(self.file_path, encoding=self.encoding)


def load_and_concatenate_data(config):
    # Step 1: Load data and dictionaries
    source_df_input_path = os.path.join(config['paths']['input_folder'], config['files']['source_dataset'])
    target_df_input_path = os.path.join(config['paths']['input_folder'], config['files']['target_dataset'])

    source_reader = CSVReader(source_df_input_path)
    target_reader = CSVReader(target_df_input_path)

    try:
        source_df = source_reader.read_csv()
        target_df = target_reader.read_csv()

        # Check if columns are the same
        if set(source_df.columns) != set(target_df.columns):
            raise ValueError("The columns of the source and target datasets do not match.")

        # Concatenate dataframes
        combined_df = pd.concat([source_df, target_df], ignore_index=True)
        return combined_df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None