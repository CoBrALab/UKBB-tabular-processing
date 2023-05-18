from __future__ import annotations

import logging
import sys
from typing import Any, TypedDict

import yaml


class Config(TypedDict):
    """defines the structure of the config dict to be passed to the function extract_UKBB_tabular_data"""

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
    Categories: list[Any] | list[None]

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

    # Drop responses such as "Do not Know" and "Prefer not to answer"
    # See code for complete list
    drop_extra_NA_codes: bool

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
    """Returns a config dict loaded from a YAML file. It is assumed the format is correct"""
    try:
        with open(config_file, "r") as stream:
            try:
                tempdict = yaml.safe_load(stream)
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
