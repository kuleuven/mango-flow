import pathlib
import re

# -*- coding: utf-8 -*-
r"""
This module contains functions to transform moustache-like patterns in strings into regex patterns.
The patterns are of the form {{variable_name}} or {{variable_name:regex expression}}.
If no regular expression is provided, a default regex pattern of  r"[\w\-\s\.]+" is used.
No spaces are allowed in the moustache patterns. 

In the explicit path variant, the extracted variables are registered in a structure that also gives the full path 
at the position where the variable was defined.

"""


def translate_mango_moustache_regex(input_string: str) -> str:
    """
    Transforms a string with moustache-like patterns into a regex pattern.
    The structure is {{variable_name}} or {{variable_name:regex expression}}.
    Note: no spaces are allowed!

    Returns:
        str: The transformed string with normal python regex patterns.
    """

    def translate_moustache_single_match(m: re.Match):
        # the matched substring, there is only one if we are called, is split in two parts:
        # the variable name and the regex expression
        # if no regex expression is provided, a default one is used
        # the default regex expression is r"[\w\-\s\.]+"
        variable_name, *regex_expression = m.group(1).split(":", 1)
        regex_expression = regex_expression[0] if regex_expression else r"[\w\.\-\s]+"
        return f"(?P<{variable_name}>{regex_expression})"

    return re.sub(r"{{([^({{)]+)}}", translate_moustache_single_match, input_string)


def mango_flow_regex_search(mango_regex_pattern: str, input_string: str) -> dict | None:
    """
    The mango_regex_pattern can be full python regex"patterns or te simpler moustache syntax.
    If the moustache syntax is used, it will be transformed internally into a full regex pattern.
    """
    regex_pattern = translate_mango_moustache_regex(mango_regex_pattern)
    if result := re.search(regex_pattern, input_string):
        return result.groupdict()
    # if no match is found, return None
    # this is the same as returning an empty dict, but None is more explicit
    return None


def get_sub_paths_with_positions(path: str) -> dict:
    """
    Returns a dict with the sub paths and the positions of the last part in the input path.
    The keys are tuples of (start, end) positions and the values are the sub paths.
    """

    path_parts = pathlib.PurePosixPath(path).parts
    sub_paths_with_last_part_positions = {}
    previous_length = 0
    for i in range(len(path_parts)):
        sub_path = str(pathlib.PurePosixPath(*path_parts[: i + 1]))
        sub_paths_with_last_part_positions[(previous_length, len(sub_path))] = sub_path
        previous_length = len(sub_path)
    return sub_paths_with_last_part_positions


def mango_flow_regex_search_path(
    mango_regex_pattern: str,
    path: str,
) -> dict[str, str] | None:
    r"""
    Like `mango_flow_regex_search`, but specifically designed for path-like strings for which a sub path
    is to be used as well (for example to fetch metadata).

    It will return a dict with the specified variable names as keys and as
    values a two element dict with 'value' and 'path' as fixed keys. 'path' will contain the full path to the part
    where the variable was used

    for example the input pattern
        `r"{{user}}/album-{{album}}/{{astro_picture:[\w\s\.]+}}$"`
    and the input path
        `"/set/home/u0123318/album-planets/saturn.jpg"`
    will return the dict
    ```python
    {
        "user": {"value": "u0123318", "path": "/set/home/u0123318"},
        "album": {"value": "planets", "path": "/set/home/u0123318/album-planets"},
        "astro_picture": {"value": "saturn.jpg", "path": "/set/home/u0123318/album-planets/saturn.jpg"}
    }
    ```
    """

    regex_pattern = translate_mango_moustache_regex(mango_regex_pattern)
    if match := re.search(regex_pattern, path):
        parent_tree_positions = get_sub_paths_with_positions(path)
        group_dict_with_paths = {}
        for key, value in match.groupdict().items():
            (min, max) = match.span(key)
            for interval in parent_tree_positions.keys():
                if min >= interval[0] and max <= interval[1]:
                    # if the match is within the interval, we can use the parent tree position
                    # to get the full path to the variable
                    group_dict_with_paths[key] = {
                        "value": value,
                        "path": parent_tree_positions[interval],
                    }
                    break
        return group_dict_with_paths
    return None
