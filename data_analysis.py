#!/usr/bin/env python3
"""
Data Analysis Script for Ordered Data
Analyzes all CSV files in the orderedData folder using Polars
Filters data to exclude 'prestart' and 'postrace' legs
Shows min, max, mean, median, std, count, and null values for each column
"""

import polars as pl
import os
from pathlib import Path
import sys


def analyze_csv_file(file_path):
    """
    Analyze a single CSV file and return comprehensive statistics
    Only analyzes data where leg is not 'prestart' or 'postrace'
    """
    try:
        print(f"\n{'='*60}")
        print(f"ANALYZING: {file_path.name}")
        print(f"{'='*60}")
        
        # Read the CSV file with flexible schema to handle mixed data types
        try:
            df = pl.read_csv(file_path)
        except Exception as e:
            print(f"Error reading with default settings, trying with flexible schema...")
            # Try reading with schema overrides for problematic columns
            df = pl.read_csv(
                file_path,
                schema_overrides={"maneuver": pl.Utf8}  # Force maneuver column to string
            )
        
        original_rows = df.shape[0]
        
        # Filter data: exclude prestart and postrace
        # Handle null values in leg column
        df_filtered = df.filter(
            (pl.col("leg").is_not_null()) & 
            (pl.col("leg") != "prestart") & 
            (pl.col("leg") != "postrace")
        )
        
        filtered_rows = df_filtered.shape[0]
        excluded_rows = original_rows - filtered_rows
        
        print(f"Original shape: {original_rows:,} rows × {df.shape[1]} columns")
        print(f"Filtered shape: {filtered_rows:,} rows × {df_filtered.shape[1]} columns")
        print(f"Excluded rows (prestart/postrace/null): {excluded_rows:,} ({excluded_rows/original_rows*100:.1f}%)")
        print(f"File size: {file_path.stat().st_size / (1024*1024):.2f} MB")
        
        # Use filtered dataframe for analysis
        df = df_filtered
        
        if df.shape[0] == 0:
            print("⚠️ No data remaining after filtering!")
            return True
        
        # Show leg distribution after filtering
        leg_distribution = df.group_by("leg").agg(pl.len().alias("count")).sort("count", descending=True)
        print(f"\nLeg distribution in filtered data:")
        for row in leg_distribution.iter_rows():
            leg_name, count = row
            print(f"  {leg_name}: {count:,} rows")
        
        # Get basic info about the dataframe
        print(f"\nColumn types:")
        for col, dtype in zip(df.columns, df.dtypes):
            print(f"  {col}: {dtype}")
        
        # Separate numeric and non-numeric columns
        numeric_columns = []
        string_columns = []
        datetime_columns = []
        boolean_columns = []
        
        for col, dtype in zip(df.columns, df.dtypes):
            if dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
                numeric_columns.append(col)
            elif dtype == pl.Utf8:
                string_columns.append(col)
            elif dtype in [pl.Datetime, pl.Date]:
                datetime_columns.append(col)
            elif dtype == pl.Boolean:
                boolean_columns.append(col)
        
        # Analyze numeric columns
        if numeric_columns:
            print(f"\n{'='*40}")
            print("NUMERIC COLUMNS ANALYSIS")
            print(f"{'='*40}")
            
            numeric_stats = df.select(numeric_columns).describe()
            
            # Custom statistics for better formatting
            stats_df = df.select([
                pl.col(col).min().alias(f"{col}_min") for col in numeric_columns
            ] + [
                pl.col(col).max().alias(f"{col}_max") for col in numeric_columns
            ] + [
                pl.col(col).mean().alias(f"{col}_mean") for col in numeric_columns
            ] + [
                pl.col(col).median().alias(f"{col}_median") for col in numeric_columns
            ] + [
                pl.col(col).std().alias(f"{col}_std") for col in numeric_columns
            ] + [
                pl.col(col).count().alias(f"{col}_count") for col in numeric_columns
            ] + [
                pl.col(col).null_count().alias(f"{col}_nulls") for col in numeric_columns
            ])
            
            stats_row = stats_df.row(0)
            n_cols = len(numeric_columns)
            
            for i, col in enumerate(numeric_columns):
                print(f"\n{col}:")
                print(f"  Min:     {stats_row[i]}")
                print(f"  Max:     {stats_row[i + n_cols]}")
                print(f"  Mean:    {stats_row[i + 2*n_cols]:.4f}" if stats_row[i + 2*n_cols] is not None else "  Mean:    None")
                print(f"  Median:  {stats_row[i + 3*n_cols]}")
                print(f"  Std:     {stats_row[i + 4*n_cols]:.4f}" if stats_row[i + 4*n_cols] is not None else "  Std:     None")
                print(f"  Count:   {stats_row[i + 5*n_cols]:,}")
                print(f"  Nulls:   {stats_row[i + 6*n_cols]:,}")
        
        # Analyze string columns
        if string_columns:
            print(f"\n{'='*40}")
            print("STRING COLUMNS ANALYSIS")
            print(f"{'='*40}")
            
            for col in string_columns:
                unique_count = df.select(pl.col(col).n_unique()).item()
                null_count = df.select(pl.col(col).null_count()).item()
                most_common = df.group_by(col).agg(pl.len().alias("count")).sort("count", descending=True).head(5)
                
                print(f"\n{col}:")
                print(f"  Unique values: {unique_count:,}")
                print(f"  Null values:   {null_count:,}")
                print(f"  Most common values:")
                for row in most_common.iter_rows():
                    value, count = row
                    print(f"    '{value}': {count:,}")
        
        # Analyze boolean columns
        if boolean_columns:
            print(f"\n{'='*40}")
            print("BOOLEAN COLUMNS ANALYSIS")
            print(f"{'='*40}")
            
            for col in boolean_columns:
                value_counts = df.group_by(col).agg(pl.len().alias("count")).sort("count", descending=True)
                null_count = df.select(pl.col(col).null_count()).item()
                
                print(f"\n{col}:")
                print(f"  Null values: {null_count:,}")
                print(f"  Value distribution:")
                for row in value_counts.iter_rows():
                    value, count = row
                    print(f"    {value}: {count:,}")
        
        # Memory usage
        print(f"\n{'='*40}")
        print("MEMORY USAGE")
        print(f"{'='*40}")
        
        # Estimate memory usage (approximation)
        memory_mb = df.estimated_size() / (1024 * 1024)
        print(f"Estimated memory usage: {memory_mb:.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"Error analyzing {file_path.name}: {str(e)}")
        return False


def main():
    """
    Main function to analyze all CSV files in the orderedData folder
    """
    # Get the script directory and construct path to orderedData
    script_dir = Path(__file__).parent
    data_dir = Path("/Users/isaia/Desktop/Large-Scale/Progetto/orderedData")
    
    if not data_dir.exists():
        print(f"Error: Directory '{data_dir}' does not exist!")
        sys.exit(1)
    
    # Find all CSV files
    csv_files = list(data_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"Error: No CSV files found in '{data_dir}'!")
        sys.exit(1)
    
    print(f"Found {len(csv_files)} CSV files:")
    for file in csv_files:
        print(f"  - {file.name}")
    
    print(f"\n{'#'*80}")
    print("STARTING DATA ANALYSIS")
    print(f"{'#'*80}")
    
    # Analyze each file
    successful_analyses = 0
    total_files = len(csv_files)
    
    for csv_file in sorted(csv_files):
        if analyze_csv_file(csv_file):
            successful_analyses += 1
    
    # Summary
    print(f"\n{'#'*80}")
    print("ANALYSIS SUMMARY")
    print(f"{'#'*80}")
    print(f"Total files found: {total_files}")
    print(f"Successfully analyzed: {successful_analyses}")
    print(f"Failed analyses: {total_files - successful_analyses}")
    
    if successful_analyses == total_files:
        print("✅ All files analyzed successfully!")
    else:
        print("⚠️  Some files could not be analyzed.")


if __name__ == "__main__":
    # Check if polars is installed
    try:
        import polars as pl
    except ImportError:
        print("Error: Polars is not installed!")
        print("Please install it with: pip install polars")
        sys.exit(1)
    
    main()
