import pandas as pd
import numpy as np
import os
from datetime import datetime


class MobilityDataAnalyzer:
    """
    NYC Taxi Trip Data Analyzer - Handles data ingestion, cleaning, and feature engineering
    """

    def __init__(self, filepath):
        """
        Initialize the analyzer with data file path

        Args:
            filepath: Path to the CSV file containing taxi trip data
        """
        self.filepath = filepath
        self.df = None
        print(f"Initialized MobilityDataAnalyzer for: {filepath}")

    def load_data(self):
        """
        Load taxi trip data from CSV file

        Returns:
            self: For method chaining
        """
        print("\n" + "=" * 60)
        print("STEP 1: LOADING DATA")
        print("=" * 60)
        print(f"Reading file: {self.filepath}")

        start_time = datetime.now()

        self.df = pd.read_csv(
            self.filepath,
            parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime']
        )

        load_time = (datetime.now() - start_time).total_seconds()

        print(f"✓ Successfully loaded {len(self.df):,} rows")
        print(f"✓ Columns: {len(self.df.columns)}")
        print(f"✓ Memory usage: {self.df.memory_usage(deep=True).sum() / 1024 ** 2:.2f} MB")
        print(f"✓ Load time: {load_time:.2f} seconds")

        return self

    def clean_data(self):
        """
        Clean data by removing invalid records

        Returns:
            self: For method chaining
        """
        print("\n" + "=" * 60)
        print("STEP 2: CLEANING DATA")
        print("=" * 60)

        initial_rows = len(self.df)
        print(f"Initial row count: {initial_rows:,}")

        # Apply cleaning filters
        print("\nApplying filters:")
        print("  - Removing passenger_count <= 0")
        print("  - Removing trip_distance <= 0.1 or > 100 miles")
        print("  - Removing fare_amount <= 0 or > 500")
        print("  - Removing total_amount <= 0")
        print("  - Removing negative tips")

        self.df = self.df[
            (self.df['passenger_count'] > 0) &
            (self.df['passenger_count'] <= 6) &
            (self.df['trip_distance'] > 0.1) &
            (self.df['trip_distance'] < 100) &
            (self.df['fare_amount'] > 0) &
            (self.df['fare_amount'] < 500) &
            (self.df['total_amount'] > 0) &
            (self.df['tip_amount'] >= 0)
            ].copy()

        removed_rows = initial_rows - len(self.df)
        removal_pct = (removed_rows / initial_rows) * 100

        print(f"\n✓ Cleaned: {len(self.df):,} rows remaining")
        print(f"✓ Removed: {removed_rows:,} rows ({removal_pct:.2f}%)")

        return self

    def feature_engineering(self):
        """
        Create new features from existing data

        Returns:
            self: For method chaining
        """
        print("\n" + "=" * 60)
        print("STEP 3: FEATURE ENGINEERING")
        print("=" * 60)

        print("Creating new features:")

        # Time-based features
        print("  - Extracting hour, day_of_week, day from pickup datetime")
        self.df['hour'] = self.df['tpep_pickup_datetime'].dt.hour
        self.df['day_of_week'] = self.df['tpep_pickup_datetime'].dt.dayofweek
        self.df['day'] = self.df['tpep_pickup_datetime'].dt.day

        # Categorical features
        print("  - Creating is_weekend flag")
        self.df['is_weekend'] = self.df['day_of_week'].isin([5, 6])

        print("  - Creating is_peak flag (7-9 AM, 5-7 PM)")
        self.df['is_peak'] = self.df['hour'].isin([7, 8, 17, 18, 19])

        # Calculated metrics
        print("  - Calculating trip_duration (minutes)")
        self.df['trip_duration'] = (
                                           self.df['tpep_dropoff_datetime'] - self.df['tpep_pickup_datetime']
                                   ).dt.total_seconds() / 60

        print("  - Calculating fare_per_mile")
        self.df['fare_per_mile'] = self.df['fare_amount'] / self.df['trip_distance']

        print("  - Calculating tip_percentage")
        self.df['tip_percentage'] = (self.df['tip_amount'] / self.df['fare_amount']) * 100

        # Remove any infinity or NaN values created by calculations
        self.df = self.df.replace([np.inf, -np.inf], np.nan)
        self.df = self.df.dropna(subset=['fare_per_mile', 'tip_percentage', 'trip_duration'])

        print(f"\n✓ Added 8 new features")
        print(f"✓ Final dataset: {len(self.df):,} rows × {len(self.df.columns)} columns")
        print(
            f"\nNew columns: hour, day_of_week, day, is_weekend, is_peak, trip_duration, fare_per_mile, tip_percentage")

        return self

    def export_clean_data(self, output_path='cleaned_trips.parquet'):
        """
        Export cleaned and engineered data to Parquet file

        Args:
            output_path: Path for output file

        Returns:
            self: For method chaining
        """
        print("\n" + "=" * 60)
        print("STEP 4: EXPORTING CLEAN DATA")
        print("=" * 60)

        print(f"Saving to: {output_path}")

        self.df.to_parquet(output_path, index=False, compression='snappy')

        file_size = os.path.getsize(output_path) / (1024 ** 2)

        print(f"✓ Successfully saved!")
        print(f"✓ File size: {file_size:.2f} MB")
        print(f"✓ Rows: {len(self.df):,}")
        print(f"✓ Columns: {len(self.df.columns)}")

        # Show sample of data
        print("\nSample of cleaned data:")
        print(self.df[['tpep_pickup_datetime', 'passenger_count', 'trip_distance',
                       'fare_amount', 'hour', 'is_peak', 'fare_per_mile']].head(3))

        print("\n" + "=" * 60)
        print("DATA PROCESSING COMPLETE!")
        print("=" * 60)

        return self


# ============================================================================
# EXECUTION - Uncomment the lines below when ready to run
# ============================================================================

if __name__ == "__main__":
    # Create analyzer and run full pipeline
    analyzer = MobilityDataAnalyzer('yellow_tripdata_2015-01.csv')

    # Execute all steps
    analyzer.load_data().clean_data().feature_engineering().export_clean_data()

    print("\n✅ All steps completed successfully!")
    print("📁 Output saved to: cleaned_trips.parquet")
    print("\nNext steps:")
    print("  1. Load cleaned_trips.parquet for KPI analysis")
    print("  2. Create visualizations")
    print("  3. Run SQL analytics")
