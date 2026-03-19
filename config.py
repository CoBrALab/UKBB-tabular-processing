"""
UKBB Data Extraction Configuration

This module defines the configuration schema for UKBB data extraction
and provides a loader for YAML configuration files.

Configuration is loaded from YAML files and validated against the Config
TypedDict schema. List fields with None values are filtered out during
loading to allow for cleaner configuration files.
"""
from __future__ import annotations

import logging
import sys
from typing import TypedDict

import yaml


class Config(TypedDict):
    """
    Configuration schema for UKBB data extraction pipeline.

    This TypedDict defines all valid configuration options that can be
    specified in a YAML config file. All list fields accept int|str values
    or None, with None values filtered out during loading.

    Example YAML file:
        FieldIDs: [31, 53]
        InstanceIDs: [1, 2]
        recode_data_values: true
        wide: true

    Attributes
    ----------

    Filtering Section
    ----------------
    FieldIDs : list[int] | list[None]
        List of FieldIDs to extract. See Data_Dictionary_Showcase.tsv for IDs.
        None or empty list extracts all fields. Example: Sex = 31
    InstanceIDs : list[int] | list[None]
        Assessment instance/timepoint to extract (1-4). None or empty extracts all.
    SubjectIDs : list[int] | list[None]
        Specific subject IDs to extract. None or empty extracts all subjects.
    SubjectIDFiles : list[str] | list[None]
        Paths to text files containing SubjectIDs (one per line). Loaded and
        merged into the SubjectIDs list.
    ArrayIDs : list[int] | list[None]
        Array indices for multi-value fields. None or empty extracts all arrays.
    Categories : list[int] | list[None]
        UKBB category IDs for hierarchical field selection. The category tree
        is recursively traversed to find all descendant FieldIDs.

    Output Control Section
    ---------------------
    replicate_non_instanced : bool
        If True, replicate non-instanced fields (e.g., Sex) across all instances.
        Non-instanced fields exist only once per subject in the raw data.
    recode_field_names : bool
        If True, replace FieldID values with "Field_FieldID" format using
        the data dictionary.
    recode_data_values : bool
        If True, decode FieldValues using the Codings file, replacing coded
        values with their human-readable meanings.
    drop_empty_strings : bool
        If True, filter out rows where FieldValue is an empty string.
    drop_null_strings : list[str] | list[None]
        List of string values to treat as null (e.g., ["None", "NA", ""]).
        Rows with these values are filtered out.
    drop_null_numerics : list[float] | list[None]
        List of numeric values to treat as null (e.g., [-1, -999]).
        Rows with these values are filtered out.

    Wide Output Control
    ------------------
    wide : bool
        If True, produce a pivoted wide format DataFrame in addition to narrow.
        Each FieldID becomes a column.
    recode_wide_column_valuetypes : bool
        If True, assign proper Polars data types to wide format columns based
        on the ValueType field from the data dictionary.
    convert_compound_to_list : bool
        If True, split compound field values (comma-separated) into lists
        in wide format output.
    convert_less_than_value_integer : int
        Numeric value to substitute when FieldValue starts with "Less than"
        for Integer type fields (e.g., for censored values).
    convert_less_than_value_continuous : int | float
        Numeric value to substitute when FieldValue starts with "Less than"
        for Continuous type fields.
    """

    ## Filtering section

    # List of FieldIDs to extract, null to extract all
    # See Data_Dictionary_Showcase.tsv for a concise list
    # Ex: Sex = 31
    FieldIDs: list[int] | list[None]

    # Instance aka timepoint, 1-4, none for all
    InstanceIDs: list[int] | list[None]

    # Specific subjects to extract, null for all
    SubjectIDs: list[int] | list[None]
    SubjectIDFiles: list[str] | list[None]

    # For fields with array components, null for all
    ArrayIDs: list[int] | list[None]

    # Use pre-defined Categories of FieldIDs, added to list above, null for none
    Categories: list[int] | list[None]

    ## Output control section

    # Replicate non-instanced data (aka Sex, other single-point measurements)
    # across all instances
    replicate_non_instanced: bool

    # Use data dictionary to recode FieldIDs as <Name>_<FieldID>
    recode_field_names: bool

    # Use data dictionary and coding file to replace FieldValues with decoded entries
    recode_data_values: bool

    # Some FieldValues were saved as empty strings instead of NA, drop these
    drop_empty_strings: bool

    # List of string responses to map to Null
    drop_null_strings: list[str] | list[None]
    drop_null_numerics: list[float] | list[None]

    ## Wide output control

    # Produce a wide aka pivoted DataFrame in addition to the filtered narrow frame
    wide: bool

    # Use data dictionary to assign proper datatypes to columns in wide output
    # Only applies to binary arrow format
    recode_wide_column_valuetypes: bool

    # Attempt to split compound type FieldValues into a list in wide output
    convert_compound_to_list: bool

    # When recode_wide_column_valuetype=true some values from recode_data_values=true
    # # some values will break setting column datatypes
    # # Substitute strings to values set below
    convert_less_than_value_integer: int
    convert_less_than_value_continuous: int | float


def load_config(config_file: str) -> Config:
    """
    Load and parse a YAML configuration file for UKBB data extraction.

    This function reads a YAML file, validates that it can be parsed, and
    filters out None values from all list fields. This allows configuration
    files to include null values for cleaner syntax without affecting the
    filtering logic.

    Parameters
    ----------
    config_file : str
        Path to the YAML configuration file. The file must conform to the
        Config TypedDict schema defined in this module.

    Returns
    -------
    Config
        Configuration dictionary with all list fields filtered to remove
        None values. Ready to pass to extract_UKBB_tabular_data().

    Raises
    ------
    SystemExit
        If the config file is not found or contains invalid YAML syntax.
        Exits with code 1 and logs the error.

    Examples
    --------
    >>> config = load_config("my_analysis.yaml")
    >>> # config now has no None values in any list fields

    Notes
    -----
    The function assumes the YAML file is structurally correct (matches the
    Config schema). Type validation is performed by mypy/static analysis tools
    using the Config TypedDict, not at runtime.
    """
    try:
        with open(config_file, "r") as stream:
            try:
                tempdict = yaml.safe_load(stream)
                # Remove None values from all list fields to avoid issues
                # with filtering logic that checks for empty/non-empty lists
                for key in tempdict.keys():
                    if isinstance(tempdict[key], list):
                        tempdict[key] = [i for i in tempdict[key] if i is not None]
                return tempdict
            except yaml.YAMLError as exc:
                logging.exception(exc)
                sys.exit(1)
    except FileNotFoundError as exc:
        logging.exception(exc)
        sys.exit(1)
