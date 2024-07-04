import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import logging
import time

class AddressNormalization:
    def __init__(self, dataframe):
        self.df = dataframe
        logging.info("AddressNormalization initialized.")

    def preprocess_string_columns(self):
        """Preprocess all string columns: convert to lowercase and remove non-alphanumeric characters"""
        for col in self.df.select_dtypes(include='object'):
            self.df[col] = self.df[col].str.lower().str.replace('[^a-z0-9\s]', '', regex=True).fillna('')

    def normalize_addresses(self, address_column):
        """Normalize addresses using geocoding and add geocode columns"""
        self.preprocess_string_columns()

        geolocator = Nominatim(user_agent="address_normalizer")

        def geocode_address(address):
            try:
                location = geolocator.geocode(address)
                if location:
                    return location.latitude, location.longitude
                else:
                    return None, None
            except GeocoderTimedOut:
                time.sleep(1)
                return geocode_address(address)

        latitudes = []
        longitudes = []
        for address in self.df[address_column]:
            lat, lon = geocode_address(address)
            latitudes.append(lat)
            longitudes.append(lon)

        self.df['geocode_latitude'] = latitudes
        self.df['geocode_longitude'] = longitudes
        logging.info("Addresses normalized.")

    def export_results(self, output_path):
        """Export the DataFrame with normalized addresses and geocodes to a CSV file"""
        try:
            self.df.to_csv(output_path, index=False)
            logging.info(f"Data exported to {output_path}")
        except Exception as e:
            logging.error(f"Error exporting data: {e}")
