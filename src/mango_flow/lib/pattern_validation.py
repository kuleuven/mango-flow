import pathlib
import re
from collections.abc import Callable
from datetime import datetime
from typing import Optional

import jinja2
from irods.session import iRODSSession


class Pattern:
    """A combination of regular expression pattern,
    additional functions for validation
    (returning a message when something is wrong,
    nothing when everything is fine)
    and jinja templates to build a destination path."""

    def __init__(
        self,
        pattern: str = r".*/(?P<filename>[\w.]+)",
        additional_validation: Optional[list[Callable]] = None,
        destination_pattern: Optional[str] = "{{filename}}",
        jinja_environment: Optional[jinja2.Environment] = None,
    ):
        self.pattern = pattern
        self.additional_validation = additional_validation or []
        # for when validation succeeds
        self.destination_pattern = destination_pattern
        self.jinja_environment = (
            jinja_environment or jinja2.Environment()
        )  # can be overriden when adding to a patternset

    def validate_against_regex(self, filename: str):
        """Basic regular expression match;
        the named groups are added as parameters of the Filename"""
        pattern_match = re.match(self.pattern, filename)
        if pattern_match is None:
            return False
        return pattern_match.groupdict()

    def run_additional_validation(self, filename_properties: dict):
        """Valid functions should take a dictionary as input,
        assuming at least the 'filename' key with the path as value,
        plus any other groups from the pattern matching and additional
        parameters the Filename class may have obtained.
        They should return a message if validation fails, otherwise nothing.
        They can also be used to modify the filename properties.
        """
        for validation_function in self.additional_validation:
            validation_result = validation_function(filename_properties)
            if validation_result is not None:
                return validation_result


class PatternSet:
    """A collection of patterns with a method for preprocessing
    (e.g. normalizing) strings,
    with optionally a shared Jinja Environment
    with filters and validation functions,
    to be combined with a filename
    so that validation loops over possible patterns."""

    def __init__(
        self,
        validation_functions: Optional[list[Callable]] = None,
        filters: Optional[dict[str, Callable]] = None,
    ):
        self.jinja_environment = jinja2.Environment()
        self._patterns = []
        self.validation_functions = validation_functions or []
        if filters is not None:
            for name, filter in filters.items():
                self.add_jinja_filter(name, filter)

    def preprocess_string(self, string: str) -> str:
        return string

    def add_pattern(self, pattern: Pattern, inherit_jinja_environment: bool = True):
        """Appy Pattern-set level validations to a pattern,
        optionally shared jinja environment,
        and add to the list of patterns."""
        for validation_function in self.validation_functions:
            if validation_function not in pattern.additional_validation:
                pattern.additional_validation.append(validation_function)
        if inherit_jinja_environment:
            pattern.jinja_environment = self.jinja_environment
        self._patterns.append(pattern)

    def add_jinja_filter(self, name: str, func: Callable):
        self.jinja_environment.filters[name] = func

    @property
    def patterns(self) -> list[Pattern]:
        return self._patterns

    @patterns.setter
    def patterns(self, patterns: list[Pattern]):
        for pattern in patterns:
            self.add_pattern(pattern)


class Filename:
    """Combination of a filename to be validated with
    instructions to build a destination path when it is invalid (using jinja)
    and optional parameters for building it.
    The validation function takes a PatternSet and loops over patterns to validate
    and construct a log dictionary to report success or failure."""

    jinja_environment = jinja2.Environment()

    def __init__(
        self,
        path: str,
        params: Optional[dict] = None,
        invalid_destination_pattern: Optional[str] = "{{filename}}",
    ):
        self.path = path
        self.params = {"filename": path.split("/")[-1]}
        if params is not None:
            self.params |= params
        self.destination_path = invalid_destination_pattern
        self.comments = []
        self.valid = False

    @property
    def destination_path(self) -> str:
        """Fill the jinja template with actual parameters."""
        if self._destination_path == "":
            return ""
        return str(pathlib.Path(self._destination_path.render(**self.params)))

    @destination_path.setter
    def destination_path(self, invalid_destination_pattern: str) -> str:
        """Create actual jinja template from string"""
        self._destination_path = (
            self.jinja_environment.from_string(invalid_destination_pattern)
            if invalid_destination_pattern
            else ""
        )

    @property
    def message(self) -> str:
        return ", ".join(self.comments)

    def validate(self, patternset: PatternSet) -> dict:
        """Preprocess (normalize) path, loop over patterns to validate"""
        string_to_validate = patternset.preprocess_string(self.path)
        for pattern in patternset.patterns:
            validation_result = self.validate_against_pattern(
                pattern, string_to_validate
            )
            if validation_result:
                return validation_result
        if not self.valid and not self.comments:
            self.comments.append("No acceptable pattern was matched.")
        return self.log_result()

    def validate_against_pattern(
        self, pattern: Pattern, string_to_validate: str = None
    ) -> dict:
        """Validate against a specific Pattern"""
        if params := pattern.validate_against_regex(string_to_validate or self.path):
            self.params.update(**params)
            additional_validation = pattern.run_additional_validation(self.params)
            if additional_validation is not None:  # something went wrong
                self.comments.append(additional_validation)
                return self.log_result()
            self.assign_valid_pattern(pattern)
            return self.log_result()
        return False

    def assign_valid_pattern(self, pattern: Pattern):
        """What to do if a pattern is a match"""
        self.jinja_environment.filters |= (
            pattern.jinja_environment.filters
        )  # update filters
        self.destination_path = pattern.destination_pattern  # provide new template
        if self.path.split("/")[-1] != self.destination_path.split("/")[-1]:
            self.comments.append("Filename has changed.")
        self.valid = True

    def log_result(self) -> dict:
        return {
            "timestamp": str(datetime.now()),
            "path": self.path,
            "destination_path": self.destination_path,
            "message": self.message,
            "succeeded": self.valid,
        }

    def postprocess_in_irods(
        self, irods_session: Optional[iRODSSession] = None
    ) -> dict:
        return self.log_result()
