import pandas as pd
import logging

class DataFrameProcessor:
    def __init__(self, df):
        self.df = df
        logging.info("DataFrameProcessor initialized.")

    def drop_columns_with_high_nas(self, threshold=0.6):
        """Drop columns from DataFrame with more than threshold NaN values and fill remaining NaNs with empty string"""
        try:
            # Calculate the number of NaNs in each column
            na_counts = self.df.isna().sum()

            # Determine which columns to drop based on the threshold
            columns_to_drop = na_counts[na_counts / len(self.df) > threshold].index

            # Drop the columns
            self.df.drop(columns=columns_to_drop, inplace=True)

            # Fill remaining NaN values with empty strings (left intentionally out)
            #self.df.fillna('', inplace=True)

            logging.info(
                f"Dropped columns with more than {threshold * 100}% NaN values and filled remaining NaNs with empty string.")
        except Exception as e:
            logging.error(f"Error dropping columns with high NaNs: {e}")

    def filter_and_save_df(self, columns_dict, output_path):
        """Filters the DataFrame to only include columns that are keys in the provided dictionary and saves it."""
        try:
            filtered_cols = [col for col in self.df.columns if col in columns_dict.keys()]
            if not filtered_cols:
                raise ValueError("No columns in the DataFrame correspond to the keys in the dictionary.")
            filtered_df = self.df[filtered_cols]
            if output_path.endswith('.csv'):
                filtered_df.to_csv(output_path, index=False)
            elif output_path.endswith('.xlsx'):
                filtered_df.to_excel(output_path, index=False)
            else:
                raise ValueError("Output file format not supported. Please use .csv or .xlsx.")
            logging.info(f"Filtered DataFrame saved to {output_path}")
        except Exception as e:
            logging.error(f"Error filtering and saving DataFrame: {e}")
