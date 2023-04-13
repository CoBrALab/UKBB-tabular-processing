from __future__ import annotations

from collections import UserDict
from typing import Any

import logging
import sys

import yaml


class Config(UserDict):
    """defines the structure of the config object to be passed to the function extract_UKBB_tabular_data"""

    FieldIDs: list[int]
    InstanceIDs: list[int] | list[None]
    SubjectIDs: list[int] | list[None]
    ArrayIDs: list[int] | list[None]
    Categories: list[Any]
    recode_field_names: bool
    recode_data_values: bool
    drop_empty_strings: bool
    drop_extra_NA_codes: bool
    wide: bool
    recode_wide_column_valuetypes: bool
    convert_compound_to_list: bool
    convert_less_than_value_integer: Any
    convert_less_than_value_continuous: Any

    @classmethod
    def from_yaml(cls, config_file: str) -> Config:
        try:
            with open(config_file, "r") as stream:
                try:
                    return cls(yaml.safe_load(stream))
                except yaml.YAMLError as exc:
                    logging.exception(exc)
                    sys.exit(1)
        except FileNotFoundError as exc:
            logging.exception(exc)
            sys.exit(1)

