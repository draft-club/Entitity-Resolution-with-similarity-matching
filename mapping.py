import json
import logging

class DataFrameMapper:
    def __init__(self, df, dict_path):
        self.dict_path = dict_path
        self.df = df
        self.mapping_dict = self.load_mapping_dict()
        logging.info("DataFrameMapper initialized.")

    def load_mapping_dict(self):
        """Load the mapping dictionary from the specified JSON path."""
        try:
            with open(self.dict_path, 'r') as file:
                return json.load(file)
        except Exception as e:
            logging.error(f"Failed to load mapping dictionary: {e}")
            raise

    def apply_mapping(self):
        """Apply the mapping from the dictionary to the dataframe columns."""
        try:
            for new_col, details in self.mapping_dict.items():
                original_col = details['features'][0]
                if original_col in self.df.columns:
                    self.df.rename(columns={original_col: new_col}, inplace=True)
            logging.info("Mapping applied successfully.")
        except Exception as e:
            logging.error(f"Error applying mapping: {e}")

    def filter_columns(self):
        """Filter the DataFrame to keep only the columns listed in the mapping dictionary keys."""
        try:
            keep_columns = [self.mapping_dict[key]['features'][0] for key in self.mapping_dict if self.mapping_dict[key]['features'][0] in self.df.columns]
            self.df = self.df[keep_columns]
            logging.info("Columns filtered successfully.")
        except Exception as e:
            logging.error(f"Error filtering columns: {e}")
