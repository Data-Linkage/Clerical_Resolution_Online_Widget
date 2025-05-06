"""Converts clustered data into pairwise format for CROW1.

The script can be used with a CSV file or a directory of CSV files
containing data in a format suitable for the cluster version of CROW1.
The data will need to contain the following columns:

- Cluster ID/number
    The unique identifier for each cluster, i.e. records with the same
    cluster ID/number that have been linked as possibly the same entity.
- Source ID
    The ID of the data source that a given record has originated from
    e.g. records from the Census may have a value of `cen` in this
    column.

If a directory is used, the cluster ID and source ID column names must
match across files.

Usage Instructions
------------------
1. Ensure that your CSV files are formatted correctly with the required
   columns:
    - Cluster ID/number
    - Source ID
2. Run the script in a terminal by entering the directory that contains
   the script (using the command `cd path/to/the/script`) and executing
   the command `python script_name.py`. Provide the path to a CSV file
   or a directory containing CSV files when prompted.
3. If a file is provided:
    - The script will load the CSV file.
    - You will be prompted to enter the cluster ID and source ID column
      names.
    - The script will standardise the data, group it by cluster and
      source, pivot it into a wide format, and reorder the columns.
4. If a directory is provided:
    - The script will load a sample CSV file from the directory for
      column validation.
    - The column selection and data processing steps will be the same as
      above.
5. The processed file(s) will be saved in a `cluster_to_pairwise_output`
   directory within the same directory as the input file or directory.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def load_csv(file_path: Path) -> pd.DataFrame:
    """Load the CSV and fill missing values with an empty string.

    Parameters
    ----------
    file_path : Path
        The location of the CSV file.

    Returns
    -------
    pd.DataFrame
        A DataFrame with nulls replaced with empty strings if a CSV file
        exists at `file_path`.
    """
    # Check if the file exists and is a file.
    if file_path.exists() and file_path.is_file():
        # Read the CSV file into a pandas DataFrame.
        data = pd.read_csv(file_path)
        # If the DataFrame is not None, fill missing values with empty
        # strings.
        if data is not None:
            return data.fillna("")
    # If the file does not exist or is not a file, raise a
    # FileNotFoundError.
    error_message = f"File not found or is not a file: {file_path}"
    raise FileNotFoundError(error_message)


def get_valid_path() -> Path:
    """Get the path to a directory or CSV file via user input.

    Returns
    -------
    Path
        The path to the directory or CSV file.
    """
    # Prompt the user to enter the path to a directory or CSV file.
    while True:
        user_input = input(
            "Enter the path to a directory or CSV file (or press Enter to quit): "
        ).strip()
        # If no input is provided, exit the program.
        if not user_input:
            print("No input provided. Exiting program...")
            sys.exit()
        path = Path(user_input)
        # If the path does not exist, prompt the user to try again.
        if not path.exists():
            print(f"Path not found: {path}. Please try again.")
            continue

        # If the path is a file, check if it has a .csv extension.
        if path.is_file():
            if path.suffix.lower() != ".csv":
                print(
                    f"The file {path} does not have a .csv extension. Please try again."
                )
                continue
            return path

        # If the path is a directory, check for CSV files in the
        # directory.
        if path.is_dir():
            csv_files = list(path.glob("*.csv"))
            if not csv_files:
                print(f"No CSV files found in directory: {path}. Please try again.")
                continue
            return path

        # If the path is neither a file nor a directory, prompt the user
        # to try again.
        print("The provided path is neither a file nor a directory. Please try again.")


def get_valid_cluster_col(data: pd.DataFrame) -> str:
    """Get a valid cluster ID column name via user input.

    Parameters
    ----------
    data : pd.DataFrame
        The pandas DataFrame to be checked.

    Returns
    -------
    str
        The standardised cluster ID column name.
    """
    while True:
        # Prompt the user to enter the cluster ID column name.
        cluster_input = input(
            "Enter the cluster ID column name (or press Enter to quit): "
        ).strip()
        # If no input is provided, exit the program.
        if not cluster_input:
            print("No cluster ID column name entered. Exiting program...")
            sys.exit()
        # If the input column name exists in the DataFrame, return it in
        # a standardised format.
        if cluster_input in data.columns:
            return cluster_input.lower().replace(" ", "_")
        # If the input column name does not exist, prompt the user to
        # try again.
        print(
            f"Column '{cluster_input}' not found in the data. Available columns: "
            f"{list(data.columns)}"
        )


def get_valid_source_col(data: pd.DataFrame) -> str:
    """Get a valid source ID column name via user input.

    Parameters
    ----------
    data : pd.DataFrame
        The pandas DataFrame to be checked.

    Returns
    -------
    str
        The standardised source ID column name.
    """
    while True:
        # Prompt the user to enter the source ID column name.
        source_input = input(
            "Enter the source ID column name (or press Enter to quit): "
        ).strip()
        # If no input is provided, exit the program.
        if not source_input:
            print("No source ID column name entered. Exiting program...")
            sys.exit()
            # If the input column name exists in the DataFrame, return
            # it in a standardised format.
        if source_input in data.columns:
            return source_input.lower().replace(" ", "_")
        # If the input column name does not exist, prompt the user to
        # try again.
        print(
            f"Column '{source_input}' not found in the data. Available columns: "
            f"{list(data.columns)}"
        )


def standardise_data(data: pd.DataFrame) -> pd.DataFrame:
    """Standardise the column names and values.

    Strip whitespace, convert to lowercase, and replace spaces with
    underscores.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame whose column names and values will be
        standardised.

    Returns
    -------
    pd.DataFrame
        The DataFrame with standardised column names and values.
    """
    # Standardise the column names by stripping whitespace, converting
    # to lowercase, and replacing spaces with underscores.
    data.columns = data.columns.str.strip().str.lower().str.replace(" ", "_")
    # Apply the `standardise_value` function to each element in the
    # DataFrame.
    return data.map(standardise_value)


def standardise_value(value: int | str) -> str:
    """Standardise the value in a DataFrame.

    Strip whitespace, convert to lowercase, and remove commas.

    Parameters
    ----------
    value : int | str
        The given value within a DataFrame to be standardised.

    Returns
    -------
    str
        The standardised value.
    """
    # If the value is not a string, convert it to a string.
    if not isinstance(value, str):
        value = str(value)
    # Strip whitespace, convert to lowercase, and remove commas from the
    # value.
    return value.strip().lower().replace(",", "")


def aggregate_values(series: pd.Series[str]) -> str:
    """Aggregate a pandas Series into a string of unique values.

    Parameters
    ----------
    series : pd.Series[str]
        A pandas Series, containing strings, whose values will be
        aggregated.

    Returns
    -------
    str
        A list of unique values in `series` separated by commas.
    """
    # Get the unique values from the Series and convert them to a list.
    unique_vals = series.unique().tolist()
    # Join the unique values into a single string, separated by commas.
    return ", ".join(unique_vals)


def group_data(
    data: pd.DataFrame, cluster_col: str, source_col: str
) -> tuple[pd.DataFrame, list[str]]:
    """Group the DataFrame by cluster and source.

    Parameters
    ----------
    data : pd.DataFrame
        The pandas DataFrame whose data will be grouped.
    cluster_col : str
        The name of the cluster ID column.
    source_col : str
        The name of the source ID column.

    Returns
    -------
    grouped_df, agg_cols : tuple[pd.DataFrame, list[str]]
        A tuple containing the DataFrame grouped by the source ID and
        cluster ID columns, and a list of columns excluding the source
        and cluster ID columns.
    """
    # Define the columns to group by (cluster and source).
    group_cols = [cluster_col, source_col]
    # Define the columns to aggregate (all columns except the group
    # columns).
    agg_cols = [col for col in data.columns if col not in group_cols]
    # Create a dictionary mapping each column to the aggregate function.
    agg_dict = dict.fromkeys(agg_cols, aggregate_values)
    # Group the DataFrame by the cluster and source columns and apply
    # the aggregation.
    grouped_df = data.groupby(group_cols, as_index=False).agg(agg_dict)
    # Return the grouped DataFrame and the list of aggregated columns.
    return grouped_df, agg_cols


def pivot_data(
    df_grouped: pd.DataFrame, cluster_col: str, source_col: str, agg_cols: list[str]
) -> pd.DataFrame:
    """Pivot the grouped DataFrame into wide-file format.

    Parameters
    ----------
    df_grouped : pd.DataFrame
        The grouped DataFrame to pivot.
    cluster_col : str
        The column name to use as the index in the pivoted DataFrame.
    source_col : str
        The column name to use as the key to group by on the pivot table
        index.
    agg_cols : list[str]
        The column names to aggregate in the pivoted DataFrame.

    Returns
    -------
    pd.DataFrame
        The pivoted DataFrame in wide-file format with reset index.
    """
    # Create a pivot table with the specified index, columns, and
    # values.
    wide_df = df_grouped.pivot_table(
        index=cluster_col, columns=source_col, values=agg_cols, aggfunc="first"
    )
    # Flatten the MultiIndex columns and format them as 'column_agg'.
    wide_df.columns = [f"{col[0]}_{col[1]}" for col in wide_df.columns]
    # Reset the index of the pivoted DataFrame.
    return wide_df.reset_index()


def reorder_columns(
    df_wide: pd.DataFrame,
    cluster_col: str,
    agg_cols: list[str],
    sources: list[str],
) -> pd.DataFrame:
    """Reorder columns by data source.

    Parameters
    ----------
    df_wide : pd.DataFrame
        The wide-format DataFrame with columns to reorder.
    cluster_col : str
        The column name to use as the index in the reordered DataFrame.
    agg_cols : list[str]
        The column names to aggregate in the reordered DataFrame.
    sources : list[str]
        The source names to use for reordering columns.

    Returns
    -------
    pd.DataFrame
        The DataFrame with columns reordered by data source.
    """
    # Initialise the final columns list with the cluster column.
    final_cols = [cluster_col]
    # Iterate over each source.
    for src in sources:
        # Iterate over each aggregation column.
        for col in agg_cols:
            # Construct the column name in the format 'col_source'.
            col_name = f"{col}_{src}"
            # If the column name exists in the DataFrame, add it to the
            # final columns list.
            if col_name in df_wide.columns:
                final_cols.append(col_name)
    # Return the DataFrame with columns reordered according to the final
    # columns list.
    return df_wide[final_cols]


def save_processed_data(
    input_file: Path, processed_data: pd.DataFrame, output_dir: Path
) -> None:
    """Save the processed DataFrame to a CSV.

    Parameters
    ----------
    input_file : Path
        The input file path.
    processed_data : pd.DataFrame
        The processed pandas DataFrame.
    output_dir : Path
        The output directory path.
    """
    # Create the output directory if it does not exist.
    output_dir.mkdir(exist_ok=True)
    # Construct the output file path using the input file stem and
    # suffix.
    output_file = output_dir / f"{input_file.stem}_pairwise{input_file.suffix}"
    # Save the processed DataFrame to the output file in CSV format.
    processed_data.to_csv(output_file, index=False)
    # Print a message indicating the file has been saved.
    print(f"Saved processed file: {output_file}")


def main() -> None:
    """Entry point for the script."""
    # Get the path input from the user.
    path_input = get_valid_path()

    # If the path input is a file, process the single file.
    if path_input.is_file():
        # Define the output directory based on the input file's parent
        # directory.
        output_dir = path_input.parent / "cluster_to_pairwise_output"
        try:
            # Load the CSV file.
            data = load_csv(path_input)
            print(f"CSV file loaded successfully: {path_input.name}")
        except FileNotFoundError as e:
            # Handle file not found error.
            print(e)
            return

        # Get the valid cluster and source column names from the user.
        cluster_col = get_valid_cluster_col(data)
        source_col = get_valid_source_col(data)

        # Standardise the data.
        data = standardise_data(data)
        # Get the unique source names.
        sources = list(data[source_col].unique())
        # Group the data by cluster and source column.
        df_grouped, agg_cols = group_data(data, cluster_col, source_col)
        # Pivot the grouped data into wide-file format.
        df_wide = pivot_data(df_grouped, cluster_col, source_col, agg_cols)
        # Reorder the columns by data source.
        df_wide_ordered = reorder_columns(
            df_wide, cluster_col, agg_cols, sources=sources
        )
        # Save the processed data to the output directory.
        save_processed_data(path_input, df_wide_ordered, output_dir)

    # If the path input is a directory, process all CSV files in the
    # directory.
    elif path_input.is_dir():
        # Define the output directory based on the input directory.
        output_dir = path_input / "cluster_to_pairwise_output"
        # Get the list of CSV files in the directory.
        csv_files = list(path_input.glob("*.csv"))
        try:
            # Load a sample CSV file for column validation.
            sample_data = load_csv(csv_files[0])
            print(f"Using {csv_files[0].name} for column validation.")
        except FileNotFoundError as e:
            # Handle file not found error.
            print(e)
            return

        # Get the valid cluster and source column names from the user.
        cluster_col = get_valid_cluster_col(sample_data)
        source_col = get_valid_source_col(sample_data)

        # Process each CSV file in the directory.
        for csv_file in csv_files:
            try:
                # Load the CSV file.
                data = load_csv(csv_file)
            except FileNotFoundError as e:
                # Handle file not found error for individual files.
                print(f"Error loading {csv_file.name}: {e}")
                continue

            print(f"Processing file: {csv_file.name}")
            # Standardise the data.
            data = standardise_data(data)
            # Get the unique source names.
            sources = list(data[source_col].unique())
            # Group the data by cluster and source columns.
            df_grouped, agg_cols = group_data(data, cluster_col, source_col)
            # Pivot the grouped data into wide-file format.
            df_wide = pivot_data(df_grouped, cluster_col, source_col, agg_cols)
            # Reorder the columns by data source.
            df_wide_ordered = reorder_columns(
                df_wide, cluster_col, agg_cols, sources=sources
            )
            # Save the processed data to the output directory.
            save_processed_data(csv_file, df_wide_ordered, output_dir)
        print("Processing complete for all files in the directory.")


if __name__ == "__main__":
    main()
