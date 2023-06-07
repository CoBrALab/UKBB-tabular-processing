from __future__ import annotations

import logging
import sys

from typing import TypedDict, Any, get_args, get_origin, get_type_hints, cast
from types import UnionType

import yaml

from validation import all_items_match_type, get_type_name, parse_list_item_types

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
    drop_null_numerics: list[float | int] | list[None]

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
    convert_less_than_value_integer: int | None
    convert_less_than_value_continuous: int | float | None

def load_config(config_file: str) -> Config:
    """Returns a config dict loaded from a YAML file. It is assumed the format is correct"""

    # Load the data from the file
    try:
        with open(config_file, "r") as stream:
            data = yaml.safe_load(stream)
    except (FileNotFoundError, yaml.YAMLError) as exc:
        logging.exception(exc)
        sys.exit(1)
    
    # Validate the format of the loaded data
    if not isinstance(data, dict):
        raise TypeError(f"Invalid format of configuration file '{config_file}', received type '{get_type_name(data)}' not 'dict'")
    
    for key, value_type in get_type_hints(Config).items():

        # Make sure the key exists in the config
        if key not in data:
            raise ValueError(f"Config object does not contain required key: '{key}'")

        # Now, we can make sure the type is correct
        if isinstance(value_type, UnionType):
            valid_types = tuple(get_origin(x) or x for x in get_args(value_type))
        else:
            valid_types = value_type
        
        # See if the type is one of the possible base types (e.g, list, string)
        is_base_type = isinstance(data[key], valid_types)
        if not is_base_type:
            raise TypeError(f"Invalid type for '{key}', expected one of {[x.__name__ for x in valid_types]}, got '{get_type_name(data[key])}'")
        
        if isinstance(data[key], list):
            assert isinstance(value_type, UnionType), "Unless something changes, all lists are union type"

            # We need to get all of the possible types for the items in the list
            valid_item_types = parse_list_item_types(value_type)
            
            # For each type, all of the items in the list must be that type
            all_items_match_type(data[key], valid_item_types)

            # Filter none in any case, as this was the behavior written by Gabe before
            data[key] = [i for i in data[key] if i is not None]
    
    return cast(Config, data)
