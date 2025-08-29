# Filename validation with patterns and functions
Mariana Montes
2025-08-29

The code described in this document was designed based on the FOG-COA
and AID-FOG ManGO flows with the goal to address situations in which we
need to **validate filenames based on a variety of regular expressions
and custom functions and move the corresponding iRODS data objects based
on the result**.

``` python
import patterns
```

# Description

The `patterns` module defines three classes:

- `Pattern`, representing a combination of a regular expression to and
  zero or more functions to validate against.

  - The regex pattern is expected to have named capturing groups.
  - The validation functions have to take a dictionary of filename
    properties (e.g. extracted from the capturing groups) and only
    return something (a string) when validation fails.
  - It also includes a jinja template to build a destination path for
    the filename if it passes the validations.

- `PatternSet`, representing a collection of `Pattern`s to validate
  against.

  - It has a `jinja2` environment with optionally custom filters that
    can be shared by all its `Pattern`s.
  - It can have validation functions to be shared by all its `Pattern`s.
  - It can have a normalization function to pre-process the path before
    it goes into a validation loop.

- `Filename`, representing a path to validate and eventually move.

  - It has a `path` attribute with the original path.
  - It has a `params` dictionary with at least the “filename” key and
    the name of the file as value. More parameters can be set manually
    or, by successful validation against a `Pattern`, from the named
    groups of the regular expressions or even the validation functions.
  - It has a basic `jinja2` environment used to fill in a
    `destination_path` template, meant for when no validation passes.
  - It can be validated against a `Pattern`; if it passes, its `params`
    are updated and it inherits the `Pattern`’s `jinja2` environment and
    `destination_path` template.
  - A `log_result()` method defines a dictionary of information to
    report on the conditions of the `Filename`.
  - The `postprocess_in_irods()` method is the only one that actually
    requires an `iRODSSession`; it allows us to do something with iRODS
    if necessary and returns the same log dictionary.

# Examples

## The `Filename` class

When instantiating a `Filename` class, you need at least a path, but you
can optionally provide a dictionary of properties `params` and a custom
`jinja2` template as `invalid_destination_pattern`.

``` python
# basic filename
fname = patterns.Filename(
    "/zone/home/project/collection/objectname.txt"
    )
fname.path
fname.params
fname.destination_path
fname.log_result()
```

    '/zone/home/project/collection/objectname.txt'

    {'filename': 'objectname.txt'}

    'objectname.txt'

    {'timestamp': '2025-08-29 09:26:08.951812',
     'path': '/zone/home/project/collection/objectname.txt',
     'destination_path': 'objectname.txt',
     'message': '',
     'succeeded': False}

We can change the destination path whenever we want; in fact, when the
file passes validation its pattern will override it.

``` python
fname.destination_path = "/zone/home/project/bad_files/{{filename}}"
fname.log_result()
```

    {'timestamp': '2025-08-29 09:26:08.975184',
     'path': '/zone/home/project/collection/objectname.txt',
     'destination_path': '/zone/home/project/bad_files/objectname.txt',
     'message': '',
     'succeeded': False}

These attributes can also be set on initiation.

``` python
fname2 = patterns.Filename(
    path="/zone/home/project/collection/dataset.csv",
    params={"project": "/zone/home/project/"},
    invalid_destination_pattern="{{project}}/bad_files/{{filename}}",
)
fname2.log_result()
```

    {'timestamp': '2025-08-29 09:26:08.999194',
     'path': '/zone/home/project/collection/dataset.csv',
     'destination_path': '/zone/home/project/bad_files/dataset.csv',
     'message': '',
     'succeeded': False}

## The `Pattern` class

The `Pattern` class is initiated with a regular expression `pattern`, a
list of functions for `additional_validation`, a `jinja2` template for a
`destination_pattern` and a `jinja_environment`. All these arguments are
optional and have default values, so it is entirely possible to try out
the class as below:

``` python
p = patterns.Pattern()
p.pattern  # group 'filename' matching words
p.additional_validation  # empty list
p.destination_pattern  # just the filename
```

    '.*/(?P<filename>[\\w.]+)'

    []

    '{{filename}}'

A `Filename` can be validated against a `Pattern` with just the
`validate_against_pattern()` method. For a case with a simple regular
expression and no validation functions, this may be overkill. The result
of this function is the output of `log_result()` after validation.

``` python
fname.validate_against_pattern(p)
```

    {'timestamp': '2025-08-29 09:26:09.051988',
     'path': '/zone/home/project/collection/objectname.txt',
     'destination_path': 'objectname.txt',
     'message': '',
     'succeeded': True}

Let’s try creating a `Pattern` with a custom regular expression, a
validation function and a destination pattern. The function
`check_txt()` below takes the dictionary of filename properties as input
and *returns a string if something is wrong, nothing otherwise*. The
`validate_against_pattern()` method first matches the path of the
`Filename` against the regex pattern, and if it succeeds, it updates the
`params` dictionary with the values of the named groups. Only then does
it go over the list of validation functions, and if any of them fail,
breaks and considers the filename invalid.

``` python
def check_txt(params):
    if params.get("ext", "") != "txt":
        return "We only accept txt files."

p2 = patterns.Pattern(
    pattern = r"/zone/home/(?P<project>[^/]+)/.+\.(?P<ext>.+)$",
    additional_validation = [check_txt],
    destination_pattern = "/zone/home/{{project}}/good_file/{{filename}}"
    )
p2.pattern
p2.additional_validation
p.destination_pattern
```

    '/zone/home/(?P<project>[^/]+)/.+\\.(?P<ext>.+)$'

    [<function __main__.check_txt(params)>]

    '{{filename}}'

We can now validate the `Filename` against this pattern.

``` python
fname.validate_against_pattern(p2)
```

    {'timestamp': '2025-08-29 09:26:09.108390',
     'path': '/zone/home/project/collection/objectname.txt',
     'destination_path': '/zone/home/project/good_file/objectname.txt',
     'message': '',
     'succeeded': True}

Because this validation requires the extension to be “txt”, a file with
a different extension will not pass. Of course, just forcing the
extension to be “txt” can be done with the regex as well, but (i) this
is just a simple example for illustration, and (ii) validation functions
also offer custom messages.

``` python
fname2.validate_against_pattern(p2)
```

    {'timestamp': '2025-08-29 09:26:09.131934',
     'path': '/zone/home/project/collection/dataset.csv',
     'destination_path': 'project/bad_files/dataset.csv',
     'message': 'We only accept txt files.',
     'succeeded': False}

## The `PatternSet` class

There is no message update in the `Filename` log just because it has
failed to match a `Pattern` unless it has passed the regex match. The
idea is that a `Filename` might be evaluated against an ordered
collection of `Pattern`s, each with potentially different regexes and
validation functions. This collection can be gathered in a `PatternSet`.

This class can be initiated with a list of `validation_functions` and a
dictionary of `jinja2` `filters`, but both are optional and can be added
later. A single `Pattern` can be added with the `add_pattern()` method,
which allows to choose whether the `Pattern` should inherit the `jinja2`
environment of the `PatternSet` (e.g. in case you have conflicting
filters). Multiple `Pattern`s can be added with the `patterns` setter,
which forces inheritance of the filters. In both cases,
`validation_functions` of the `PatternSet` are appended to each of the
patterns (as long as the pattern is added after the function).

``` python
patternset = patterns.PatternSet()
patternset.patterns  # empty
patternset.patterns = [p2, p]
patternset.patterns
fname2.validate(patternset)
fname.validate(patternset)
```

    []

    [<patterns.Pattern at 0x7f6d44694920>, <patterns.Pattern at 0x7f6d445d52b0>]

    {'timestamp': '2025-08-29 09:26:09.183155',
     'path': '/zone/home/project/collection/dataset.csv',
     'destination_path': 'project/bad_files/dataset.csv',
     'message': 'We only accept txt files.',
     'succeeded': False}

    {'timestamp': '2025-08-29 09:26:09.188379',
     'path': '/zone/home/project/collection/objectname.txt',
     'destination_path': '/zone/home/project/good_file/objectname.txt',
     'message': '',
     'succeeded': True}
