import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from jellyfish import jaro_winkler_similarity
import logging
from constants import ADDRESS_COLUMN


class EntityResolution:
    def __init__(self, dataframe):
        self.df = dataframe
        self.tfidf_vectorizer = TfidfVectorizer(min_df=1, analyzer='word', ngram_range=(1, 3))
        logging.info("EntityResolution initialized.")

    def preprocess_data(self, column):
        """Normalize and prepare data for vectorization"""
        return self.df[column].astype(str).str.lower().str.replace('[^a-z0-9\s]', '', regex=True).fillna('')

    def vectorize_data(self, column):
        """Convert text data into TF-IDF matrix"""
        preprocessed_data = self.preprocess_data(column)
        if preprocessed_data.str.strip().eq('').all():
            logging.error(f"All preprocessed data in column '{column}' are empty.")
            raise ValueError(f"All preprocessed data in column '{column}' are empty.")
        return self.tfidf_vectorizer.fit_transform(preprocessed_data)

    def compute_cosine_similarity(self):
        """Compute cosine similarity across all text columns and average it per row"""
        text_columns = self.df.select_dtypes(include=['object']).columns
        all_similarities = []

        for column in text_columns:
            tfidf_matrix = self.vectorize_data(column)
            if tfidf_matrix is not None and tfidf_matrix.shape[0] > 0:
                sim_matrix = cosine_similarity(tfidf_matrix, tfidf_matrix)
                np.fill_diagonal(sim_matrix, 0)
                row_similarities = sim_matrix.sum(axis=1)
                all_similarities.append(row_similarities)

        all_similarities = np.array(all_similarities)
        avg_similarities = np.nanmean(all_similarities, axis=0) / len(text_columns)

        return avg_similarities

    def compute_jaro_winkler_similarity(self, column):
        """Compute Jaro-Winkler similarity for the address column"""
        if column in self.df.columns:
            similarities = self.df[column].apply(
                lambda x: np.mean([jaro_winkler_similarity(x, y) for y in self.df[column] if x != y])
            )
            return similarities
        return np.zeros(len(self.df))

    def integrate_scores(self):
        """Integrate similarity scores into the DataFrame"""
        self.df['cosine_avg_sim'] = self.compute_cosine_similarity()
        if ADDRESS_COLUMN in self.df.columns:
            self.df['jaro_avg_sim'] = self.compute_jaro_winkler_similarity(ADDRESS_COLUMN)
            self.df['combined_avg_sim'] = (self.df['cosine_avg_sim'] + self.df['jaro_avg_sim']) / 2
        else:
            self.df['combined_avg_sim'] = self.df['cosine_avg_sim']

    def sort_and_export_results(self, output_path):
        """Sort by cosine_avg_sim and export the DataFrame with similarity scores to a CSV file"""
        try:
            self.integrate_scores()
            self.df.sort_values(by='cosine_avg_sim', ascending=False, inplace=True)

            if not self.df.empty:
                self.df.to_csv(output_path, index=False)
                self.df.to_excel(output_path.replace('.csv', '.xlsx'), index=False)
                logging.info(f"Processed data sorted and saved to {output_path}")
            else:
                logging.warning("The DataFrame is empty. No file has been saved.")

        except Exception as e:
            logging.error(f"Error in sorting and exporting results: {e}")
