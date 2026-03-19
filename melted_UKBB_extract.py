#!/usr/bin/env python
"""
UKBB Tabular Data Extraction Tool

This module transforms melted UK Biobank tabular data into analysis-ready formats.
The data pipeline processes UKBB .tab files that have been converted from ultra-wide
to long/melted format by the AWK preprocessor (ukb_awk/melt_tab.awk).

The module supports:
- Filtering by SubjectID, FieldID, InstanceID, ArrayID, and hierarchical Categories
- Value recoding using UKBB data dictionary and coding mappings
- Optional pivoting to wide format with proper type assignment
- Output to TSV, Arrow/Feather, Parquet, or CSV formats

Input data can be in TSV format or compressed binary Arrow format.
All processing uses Polars LazyFrames with streaming for memory efficiency.

Required UKBB Showcase files:
- Data_Dictionary_Showcase.tsv: Field metadata (FieldID, Field, ValueType, Coding)
- Codings.tsv: Value encoding mappings (Coding, Value, Meaning)
- 13.txt: Category tree for hierarchical field selection (parent_id, child_id)
- 1.txt: Data field properties for instanced field detection (field_id, instanced)
"""
from __future__ import annotations

import logging
import pathlib as p
import sys

import pprint

import polars as pl

from config import Config, load_config


def extract_UKBB_tabular_data(
    config: Config,
    data_file: str,
    dictionary_file: str,
    coding_file: str,
    category_tree_file: str = None,
    data_field_prop_file: str = None,
    verbose: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame | None, pl.DataFrame, pl.DataFrame]:
    """
    Extract, filter, and transform UK Biobank tabular data.

    This function processes melted UKBB data through a pipeline of filtering,
    recoding, and optional pivoting operations. All operations use Polars
    LazyFrames with streaming for memory efficiency on large datasets.

    Parameters
    ----------
    config : Config
        Configuration dictionary specifying filtering and transformation options.
        See config.py for the complete schema. Key options include:
        - FieldIDs, InstanceIDs, SubjectIDs, ArrayIDs: Filter conditions
        - Categories: Hierarchical field selection (expanded recursively)
        - replicate_non_instanced: Duplicate non-instanced fields across instances
        - recode_data_values: Replace coded values with decoded meanings
        - wide: Pivot to wide format with FieldID as columns

    data_file : str
        Path to input data file. Supports:
        - .tsv: Tab-separated values (melted format)
        - .arrow/.feather: Compressed binary Arrow format

    dictionary_file : str
        Path to UKBB Data Dictionary Showcase TSV. Required columns:
        FieldID, Field, ValueType, Coding, Category

    coding_file : str
        Path to UKBB Codings TSV. Required columns:
        Coding, Value, Meaning

    category_tree_file : str, optional
        Path to UKBB Category tree (Schema 13). Required when config
        contains Categories. Format: parent_id, child_id (tab-separated)

    data_field_prop_file : str, optional
        Path to UKBB Data field properties (Schema 1). Required when
        replicate_non_instanced=True. Format: field_id, instanced

    verbose : bool, default=False
        Enable verbose Polars output for debugging

    Returns
    -------
    tuple containing:
        - data_narrow : pl.DataFrame
            Filtered long format data with columns:
            SubjectID, InstanceID, ArrayID, FieldID, FieldValue
        - data_wide : pl.DataFrame or None
            Pivoted wide format if config['wide']=True, otherwise None.
            Columns are SubjectID, InstanceID, ArrayID, plus one column
            per FieldID with proper typing applied
        - dictionary : pl.DataFrame
            Subset of data dictionary matching extracted FieldIDs
        - codings : pl.DataFrame
            Subset of codings used in extracted data

    Notes
    -----
    Processing Strategy:
    - Uses Polars LazyFrames with streaming=True and no_optimization=True
      throughout for memory efficiency on datasets exceeding RAM capacity
    - Category expansion recursively traverses the category tree to find
      all descendant FieldIDs
    - Non-instanced fields (e.g., Sex) exist only once per subject in the
      raw data but can be replicated across all instances when requested
    - InstanceID and ArrayID are cast to Categorical after collection
      to reduce memory usage

    Examples
    --------
    >>> config = load_config("myconfig.yaml")
    >>> narrow, wide, dict_, codings = extract_UKBB_tabular_data(
    ...     config=config,
    ...     data_file="current.melt.arrow",
    ...     dictionary_file="Data_Dictionary_Showcase.tsv",
    ...     coding_file="Codings.tsv",
    ...     category_tree_file="13.txt",
    ...     data_field_prop_file="1.txt"
    ... )
    """
    pl.Config.set_verbose(verbose)

    # Mapping of UKBB ValueType strings to Polars data types
    # Used when recode_wide_column_valuetypes=True to properly type pivoted columns
    datatype_dictionary = {
        "Date": pl.Date,
        "Time": pl.Datetime,
        "Continuous": pl.Float64,
        "Text": pl.Utf8,
        "Integer": pl.Int64,
        "Categorical multiple": pl.Categorical,
        "Categorical single": pl.Categorical,
        "Compound": pl.Utf8,
    }

    dictionary = pl.scan_csv(
        dictionary_file,
        separator="\t",
        infer_schema_length=None,
        encoding="utf8-lossy",
        quote_char=None,
    )

    codings = pl.scan_csv(
        coding_file,
        separator="\t",
        dtypes={
            "Coding": pl.Int64,
            "Value": pl.Utf8,
            "Meaning": pl.Utf8,
        },
        encoding="utf8-lossy",
    )

    file_extension = p.Path(data_file).suffix

    if file_extension == ".tsv":
        # We setup a LazyFrame chain of filters based on the configuration
        data = pl.scan_csv(
            data_file,
            separator="\t",
            dtypes={
                "SubjectID": pl.Int64,
                "FieldID": pl.Int64,
                "InstanceID": pl.Int64,
                "ArrayID": pl.Int64,
                "FieldValue": pl.Utf8,
            },
            encoding="utf8-lossy",
        )
    elif file_extension in [".arrow", ".feather"]:
        data = pl.scan_ipc(data_file)
    else:
        logging.error(f"Unsupported file extension: {file_extension}")
        sys.exit(1)

    # Expand list of IDs from SubjectIDFiles
    if config["SubjectIDFiles"]:
        for file in config["SubjectIDFiles"]:
            try:
                with open(file, "r") as stream:
                    config["SubjectIDs"].extend(
                        [int(x) for x in stream.read().splitlines()]
                    )
            except FileNotFoundError as exc:
                logging.exception(exc)
                sys.exit(1)
        logging.info("Input configuration after loading SubjectIDFiles")
        logging.info(pprint.pformat(config, compact=True))

    # Filter rows based on SubjectIDs if provided
    if config["SubjectIDs"]:
        data = data.filter(pl.col("SubjectID").is_in(config["SubjectIDs"]))

    # Expand FieldIDs if Categories are provided
    if config["Categories"]:
        logging.info(
            "Categories provided, recursing down Category tree to ensure all FieldIDs are discovered"
        )
        # UKBB categories form a hierarchical tree. This loop recursively finds
        # all descendants of the specified categories by iterating until no new
        # children are discovered (the Categories list stops growing).
        category_tree = pl.read_csv(category_tree_file, separator="\t")
        old_length = 0
        while len(config["Categories"]) > old_length:
            old_length = len(config["Categories"])
            config["Categories"].extend(
                category_tree.filter(
                    pl.col("parent_id").is_in(config["Categories"])
                    & pl.col("child_id").is_in(config["Categories"]).is_not()
                )
                .get_column("child_id")
                .to_list()
            )
        # After collecting all descendant category IDs, find all FieldIDs
        # belonging to those categories and add them to the filter list
        config["FieldIDs"].extend(
            dictionary.filter(pl.col("Category").is_in(config["Categories"]))
            .select("FieldID")
            .collect(streaming=True, no_optimization=True)
            .to_series()
            .to_list()
        )
        config["FieldIDs"] = list(dict.fromkeys(config["FieldIDs"]))
        # Print the loaded config
        logging.info("Input configuration after Category expansion")
        logging.info(pprint.pformat(config, compact=True))

    # Filter rows in data based on FieldID
    if config["FieldIDs"]:
        data = data.filter(pl.col("FieldID").is_in(config["FieldIDs"]))

    if config["replicate_non_instanced"]:
        # Some UKBB fields (e.g., Sex, genetic sex) are non-instanced, meaning they
        # exist only once per subject rather than at each assessment instance.
        # This section replicates those single values across all instances.
        #
        # Strategy:
        # 1. Join with field properties to identify which fields are instanced (instanced=1)
        #    vs non-instanced (instanced=0)
        # 2. For non-instanced rows, repeat the row N times where N = number of instances
        # 3. Assign sequential instance IDs to the repeated rows
        # 4. Explode lists back to regular rows
        instanced = pl.scan_csv(data_field_prop_file, separator="\t")
        data = data.join(
            instanced.select(["field_id", "instanced"]),
            left_on="FieldID",
            right_on="field_id",
            how="left",
        )
        repeat_instances = config["InstanceIDs"] if config["InstanceIDs"] else list(range(4))
        data = (
            data.with_columns(
                pl.when(pl.col("instanced") == 0)
                .then(pl.lit(len(repeat_instances)).alias("repeats"))
                .otherwise(1)
            )
            .select(pl.exclude("repeats").repeat_by("repeats"))
            .with_columns(
                pl.when(pl.col("InstanceID").list.lengths() > 1)
                .then(repeat_instances)
                .otherwise(pl.col("InstanceID"))
                .alias("InstanceID")
            )
            .explode(pl.all())
        )
        data = data.drop("instanced")

    # Filter rows based on InstanceIDs if provided
    if config["InstanceIDs"]:
        data = data.filter(pl.col("InstanceID").is_in(config["InstanceIDs"]))

    # Filter rows based on ArrayIDs if provided
    if config["ArrayIDs"]:
        data = data.filter(pl.col("ArrayID").is_in(config["ArrayIDs"]))

    # Drop empty strings
    if config["drop_empty_strings"]:
        data = data.filter(~(pl.col("FieldValue").str.lengths() == 0))

    # Join the data dictionary to the dataset
    data = data.join(
        dictionary.select(["FieldID", "Field", "ValueType", "Coding"]),
        on="FieldID",
        how="left",
    )
    data = data.join(
        codings,
        left_on=["Coding", "FieldValue"],
        right_on=["Coding", "Value"],
        how="left",
    )

    if config["drop_null_strings"]:
        data = data.filter(~pl.col("Meaning").is_in(config["drop_null_strings"]))

    if config["drop_null_numerics"]:
        data = data.filter(
            ~(
                pl.col("FieldValue")
                .cast(pl.Float64, strict=False)
                .is_in(config["drop_null_numerics"])
            )
        )

    # Take coding values and replace FieldValue with it if available
    if config["recode_data_values"]:
        data = data.with_columns(
            pl.when(pl.col("Meaning").is_not_null())
            .then(pl.col("Meaning"))
            .otherwise(pl.col("FieldValue"))
            .alias("FieldValue")
        )

    # Take coding values which start with "Less than" and replace with a numeric
    if config["convert_less_than_value_integer"] is not None:
        data = data.with_columns(
            [
                pl.when(
                    (pl.col("FieldValue").str.starts_with("Less than"))
                    & (pl.col("ValueType").is_in(["Integer"]))
                )
                .then(pl.lit(config["convert_less_than_value_integer"]))
                .otherwise(pl.col("FieldValue"))
                .keep_name()
            ]
        )

    if config["convert_less_than_value_continuous"] is not None:
        data = data.with_columns(
            [
                pl.when(
                    (pl.col("FieldValue").str.starts_with("Less than"))
                    & (pl.col("ValueType") == "Continuous")
                )
                .then(pl.lit(config["convert_less_than_value_continuous"]))
                .otherwise(pl.col("FieldValue"))
                .keep_name()
            ]
        )

    # Replace FieldID with concatenation of FieldID and Field
    if config["recode_field_names"]:
        data = data.with_columns(
            pl.concat_str([pl.col("Field"), pl.col("FieldID")], separator="_").alias(
                "FieldID"
            )
        )

    # Drop extra columns and reorder
    data = data.select(["SubjectID", "InstanceID", "ArrayID", "FieldID", "FieldValue"])

    logging.info(f"Loading data from {data_file}")
    data = data.collect(streaming=True, no_optimization=True)

    # Generate a subsetted dictionary and codings
    if config["FieldIDs"]:
        dictionary = dictionary.filter(
            pl.col("FieldID").is_in(config["FieldIDs"])
        ).collect(streaming=True, no_optimization=True)
        codings = codings.filter(
            pl.col("Coding").is_in(dictionary.get_column("Coding"))
        ).collect(streaming=True, no_optimization=True)
    else:
        dictionary = dictionary.collect(streaming=True, no_optimization=True)
        codings = codings.collect(streaming=True, no_optimization=True)

    data = data.with_columns(pl.col("InstanceID").cast(pl.Utf8).cast(pl.Categorical))
    data = data.with_columns(pl.col("ArrayID").cast(pl.Utf8).cast(pl.Categorical))

    # Optional wide format output: pivot from long to wide format
    # Each unique FieldID becomes a column, with one row per subject/instance/array
    if config["wide"]:
        logging.info("Pivoting narrow DataFrame to wide")
        data_wide = data.pivot(
            index=["SubjectID", "InstanceID", "ArrayID"],
            values="FieldValue",
            columns="FieldID",
            aggregate_function=None,
        )

        if config["recode_wide_column_valuetypes"]:
            # After pivoting, all columns are strings. This section assigns proper
            # data types based on the UKBB ValueType field from the data dictionary.
            #
            # Note: Column names may be FieldID only or Field_FieldID depending on
            # recode_field_names config, so we extract the numeric ID from the end.
            logging.info("Setting data types on columns")
            for col in data_wide.columns[3:]:
                val_type = (
                    dictionary.filter(
                        pl.col("FieldID").cast(pl.Utf8) == col.split("_")[-1]
                    )
                    .select("ValueType")
                    .item()
                )
                try:
                    if val_type == "Date":
                        data_wide = data_wide.with_columns(
                            pl.col(col).str.strptime(pl.Date)
                        )
                    elif val_type == "Time":
                        data_wide = data_wide.with_columns(
                            pl.col(col).str.strptime(pl.Datetime)
                        )
                    elif val_type == "Compound" and config["convert_compound_to_list"]:
                        # Compound fields contain comma-separated values
                        data_wide = data_wide.with_columns(pl.col(col).str.split(","))
                    else:
                        data_wide = data_wide.with_columns(
                            pl.col(col).cast(datatype_dictionary[val_type])
                        )
                except pl.exceptions.ComputeError as exe:
                    logging.warning(exe)
                    logging.warning(
                        f"Column {col} data type could not be set due to mixed value types"
                    )

        return data, data_wide, dictionary, codings

    else:
        return data, None, dictionary, codings


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog="UKBB Data Extractor",
        description="Transforms melted UKBB tabular data into a usable DataFrame for statistical analysis",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--config-file",
        help="YAML config file describing how to process UKBB table",
        required=True,
    )
    parser.add_argument("--data-file", help="UKBB melted tabular data", required=True)
    parser.add_argument(
        "--dictionary-file",
        help="UKBB data dictionary showcase file",
        default="Data_Dictionary_Showcase.tsv",
    )
    parser.add_argument("--coding-file", help="UKBB coding file", default="Codings.tsv")
    parser.add_argument(
        "--category-tree-file",
        help="UKBB Category tree file (Schema 13), tab-separated from https://biobank.ndph.ox.ac.uk/showcase/schema.cgi?id=13",
        default="13.txt",
    )
    parser.add_argument(
        "--data-field-prop-file",
        help="UKBB Data field properties file (Schema 1), tab-separated from https://biobank.ndph.ox.ac.uk/showcase/schema.cgi?id=1",
        default="1.txt",
    )
    parser.add_argument(
        "--output-prefix", help="Prefix for output files", required=True
    )

    parser.add_argument(
        "--output-formats",
        help="Specify list of output file formats from tsv, arrow/feather, parquet, csv",
        action="store",
        nargs="*",
        default=["tsv", "arrow"],
    )

    parser.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true"
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=logging.DEBUG,
        handlers=[
            logging.FileHandler(f"{args.output_prefix}conversion.log", mode="w"),
            logging.StreamHandler(),
        ],
    )

    unknown_output_formats = set(args.output_formats).difference(
        {"tsv", "csv", "arrow", "parquet", "feather"}
    )
    if unknown_output_formats:
        logging.error(
            f"Unknown output formats {pprint.pformat(unknown_output_formats, compact=True)}"
        )
        sys.exit(1)

    if "csv" in args.output_formats:
        logging.warning(
            "Due to embedded quotes in some fields, CSV format is not recommended"
        )

    config = load_config(args.config_file)

    # Print the loaded config
    logging.info("Input configuration")
    logging.info(pprint.pformat(config, compact=True))

    data, data_wide, dictionary, codings = extract_UKBB_tabular_data(
        config=config,
        data_file=args.data_file,
        dictionary_file=args.dictionary_file,
        coding_file=args.coding_file,
        category_tree_file=args.category_tree_file,
        data_field_prop_file=args.data_field_prop_file,
        verbose=args.verbose,
    )

    for format in args.output_formats:
        if format == "tsv":
            logging.info(f"Writing {args.output_prefix}narrow.tsv")
            data.write_csv(f"{args.output_prefix}narrow.tsv", separator="\t")
        elif format == "arrow" or format == "feather":
            logging.info(f"Writing {args.output_prefix}narrow.{format}")
            data.write_ipc(f"{args.output_prefix}narrow.{format}", compression="zstd")
        elif format == "parquet":
            logging.info(f"Writing {args.output_prefix}narrow.parquet")
            data.write_parquet(
                f"{args.output_prefix}narrow.parquet", compression="zstd"
            )
        elif format == "csv":
            logging.info(f"Writing {args.output_prefix}narrow.csv")
            data.write_csv(f"{args.output_prefix}narrow.csv")

    logging.info(f"Writing {args.output_prefix}dictionary.tsv")
    dictionary.write_csv(f"{args.output_prefix}dictionary.tsv", separator="\t")

    logging.info(f"Writing {args.output_prefix}coding.tsv")
    codings.write_csv(f"{args.output_prefix}coding.tsv", separator="\t")

    if data_wide is not None:
        for format in args.output_formats:
            if format == "tsv":
                logging.info(f"Writing {args.output_prefix}wide.tsv")
                data_wide.write_csv(f"{args.output_prefix}wide.tsv", separator="\t")
            elif format == "arrow" or format == "feather":
                logging.info(f"Writing {args.output_prefix}wide.{format}")
                data_wide.write_ipc(
                    f"{args.output_prefix}wide.{format}", compression="zstd"
                )
            elif format == "parquet":
                logging.info(f"Writing {args.output_prefix}wide.parquet")
                data_wide.write_parquet(
                    f"{args.output_prefix}wide.parquet", compression="zstd"
                )
            elif format == "csv":
                logging.info(f"Writing {args.output_prefix}wide.csv")
                data_wide.write_csv(f"{args.output_prefix}wide.csv")
