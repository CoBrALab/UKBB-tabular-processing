from typing import Any, get_args
from types import NoneType, UnionType


def get_type_name(obj: object) -> str:
    return obj.__class__.__name__


def parse_list_item_types(union_type: UnionType) -> list[type]:
    """given a union of list types, return a list of all of the values contained in the union members"""
    item_types: list[type] = []
    for list_type in get_args(union_type):
        for item_type in get_args(list_type):
            if item_type not in item_types:
                item_types.append(item_type)
    return item_types


def all_items_match_type(items: list[Any], types: list[type]) -> None:
    """throw if not all of the items in the list are instances one of the types, where mixed types in the list are not allowed"""
    for item_type in types:
        if item_type is None:
            item_type = NoneType
        for i, item in enumerate(items):
            if not isinstance(item, item_type):
                break
            elif i == len(items) - 1:
                return
    raise TypeError(f"List must be list of one of types: '{types}', got '{items}'")
