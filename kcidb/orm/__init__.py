"""
Kernel CI report object-relational mapping (ORM) - report data organized into
objects, but without the object-oriented interface.
"""

import re
import textwrap
import logging
import argparse
from abc import ABC, abstractmethod
import jsonschema
import kcidb.io as io
import kcidb.misc
from kcidb.misc import LIGHT_ASSERTS

# We'll get to it, pylint: disable=too-many-lines


# Module's logger
LOGGER = logging.getLogger(__name__)


class Relation:
    """A parent/child relation between object types"""

    def __init__(self, parent, child, ref_fields):
        """
        Initialize a relation.

        Args:
            parent:     The parent type.
            child:      The child type.
            ref_fields: A tuple containing the list of names of child fields,
                        containing values of parent's globally-identifying
                        fields (in the same order), and linking the two.
        """
        assert isinstance(parent, Type)
        assert isinstance(child, Type)
        assert isinstance(ref_fields, tuple)
        assert all(isinstance(ref_field, str) for ref_field in ref_fields)
        assert len(ref_fields) == len(parent.id_fields)
        self.parent = parent
        self.child = child
        self.ref_fields = ref_fields


class Type:
    """An object type"""

    # It's OK, pylint: disable=too-many-instance-attributes,too-many-arguments

    def __init__(self, name, json_schema, id_fields):
        """
        Initialize an object type.

        Args:
            name:                   The type name.
            json_schema:            The JSON schema describing the raw object
                                    data.
            id_fields:              A tuple containing the names of object
                                    fields identifying it globally.
        """
        assert isinstance(name, str)
        assert isinstance(json_schema, dict)
        assert isinstance(id_fields, tuple) and \
               all(isinstance(f, str) for f in id_fields)

        # The name of this type
        self.name = name
        # The JSON schema for this type's raw data
        self.json_schema = json_schema
        # List of ID field names
        self.id_fields = id_fields
        # A list of all relations
        self.relations = []
        # A map of parent type names and their relations
        self.parents = {}
        # A map of child type names and their relations
        self.children = {}

    def add_relation(self, relation):
        """
        Add a relation of this object to another.

        Args:
            relation:   The relation to add.
                        Must have this type as either the parent or the child.
                        Must not have been added before.
        """
        assert isinstance(relation, Relation)
        assert self is relation.parent or self is relation.child
        assert relation not in self.relations
        self.relations.append(relation)
        if self is relation.parent:
            self.children[relation.child.name] = relation
        if self is relation.child:
            self.parents[relation.parent.name] = relation

    def validate(self, data):
        """
        Validate a type's data against its JSON schema.

        Args:
            data:   The data to validate.

        Returns:
            The validated (but unmodified) data.

        Raises:
            `jsonschema.exceptions.ValidationError`, if the data did not
            adhere to this type's JSON schema.
        """
        try:
            format_checker = jsonschema.Draft7Validator.FORMAT_CHECKER
        except AttributeError:
            # Nevermind, pylint: disable=fixme
            # TODO Remove once we stop supporting Python 3.6
            format_checker = jsonschema.draft7_format_checker

        jsonschema.validate(instance=data, schema=self.json_schema,
                            format_checker=format_checker)
        return data

    def is_valid(self, data):
        """
        Check if a type's data is valid according to its JSON schema.

        Args:
            data:   The data to check.

        Returns:
            True if the data is valid, False otherwise.
        """
        try:
            self.validate(data)
            return True
        except jsonschema.exceptions.ValidationError:
            return False

    def get_id(self, data):
        """
        Retrieve a tuple of field values identifying an object globally, from
        its data.

        Args:
            data:   The object data to retrieve the ID from.

        Returns:
            A tuple of values of object fields identifying it globally.
        """
        assert LIGHT_ASSERTS or self.is_valid(data)
        return tuple(data[field] for field in self.id_fields)

    def get_parent_id(self, parent_type_name, data):
        """
        Retrieve a tuple of field values identifying an object's parent of
        particular type globally, from the object's data.

        Args:
            parent_type_name:   The name of the type of the parent object to
                                retrieve the ID of.
            data:               The object data to retrieve the parent ID
                                from.

        Returns:
            A tuple of values of object fields identifying the parent
            globally.
        """
        assert LIGHT_ASSERTS or self.is_valid(data)
        assert parent_type_name in self.parents
        return tuple(data[field]
                     for field in self.parents[parent_type_name].ref_fields)


class Schema:
    """A repository of recognized object types"""

    def __init__(self, json_schema_defs, types):
        """
        Initialize the schema.

        Args:
            json_schema_defs:   A dictionary of JSON schemas shared and
                                referenced by field schemas.
            types:  A dictionary of type descriptions. Keys being type names
                    and values being dictionaries with the following entries:
                        * "field_json_schemas" - a dictionary of field names
                                                 and JSON schemas for their
                                                 values (when present),
                        * "required_fields" - a set of names of required
                                              fields,
                        * "id_fields" - a tuple of names of the object fields
                                        identifying it globally,
                        * "children" - the optional dictionary of names of
                                       child types and tuples containing
                                       names of fields with values of parent's
                                       identifying fields ("id_fields"),
                                       in the same order.
        """
        assert isinstance(json_schema_defs, dict)
        assert isinstance(types, dict)
        assert all(
            isinstance(name, str) and
            isinstance(info, dict) and
            "field_json_schemas" in info and
            isinstance(info["field_json_schemas"], dict) and
            "required_fields" in info and
            isinstance(info["required_fields"], set) and
            "id_fields" in info and
            isinstance(info["id_fields"], tuple) and
            (set(info["id_fields"]) <= set(info["field_json_schemas"])) and
            all(isinstance(f, str) for f in info["id_fields"]) and
            ("children" not in info or (
                isinstance(info["children"], dict) and
                all(
                    isinstance(name, str) and
                    name in types and
                    isinstance(ref_fields, tuple) and
                    all(isinstance(f, str) for f in ref_fields)
                    for name, ref_fields in info["children"].items()
                )
            ))
            for name, info in types.items()
        )

        # Create types and build the JSON schema
        self.types = {}
        self.json_schema = {
            "type": "object",
            "$defs": json_schema_defs,
            "properties": {},
            "additionalProperties": False,
        }
        for name, info in types.items():
            json_schema = dict(
                type="object",
                properties={
                    name: (
                        json_schema if name in info["required_fields"]
                        else dict(anyOf=[dict(type="null"), json_schema])
                    )
                    for name, json_schema in
                    info["field_json_schemas"].items()
                },
                required=list(info["field_json_schemas"]),
                additionalProperties=False,
            )
            self.json_schema["properties"][name] = dict(
                type="array", items=json_schema.copy()
            )
            json_schema["$defs"] = json_schema_defs
            self.types[name] = Type(name, json_schema, info["id_fields"])

        # Create and register relations
        self.relations = []
        for name, info in types.items():
            type = self.types[name]
            for child_name, ref_fields in info.get("children", {}).items():
                try:
                    child_type = self.types[child_name]
                except KeyError:
                    raise Exception(f"Couldn't find child {child_name!r} "
                                    f"of type {name!r}") from None
                relation = Relation(type, child_type, ref_fields)
                self.relations.append(relation)
                type.add_relation(relation)
                child_type.add_relation(relation)

    def validate(self, data):
        """
        Validate raw object-oriented data against the schema.

        Args:
            data:   The data to validate.

        Returns:
            The validated data.

        Raises:
            `jsonschema.exceptions.ValidationError`, if the data did not
            match the schema.
        """
        try:
            format_checker = jsonschema.Draft7Validator.FORMAT_CHECKER
        except AttributeError:
            # Nevermind, pylint: disable=fixme
            # TODO Remove once we stop supporting Python 3.6
            format_checker = jsonschema.draft7_format_checker

        jsonschema.validate(instance=data, schema=self.json_schema,
                            format_checker=format_checker)
        return data

    def is_valid(self, data):
        """
        Check if a raw object-oriented data is valid according to the schema.

        Args:
            data:   The data to check.

        Returns:
            True if the data is valid, False otherwise.
        """
        try:
            self.validate(data)
            return True
        except jsonschema.exceptions.ValidationError:
            return False

    def format_dot(self):
        """
        Format the directed graph of object type relations in the schema using
        the DOT language. The returned value could be visualized with e.g.
        "dot -Tx11".

        Returns:
            The string containing the DOT representation of object type
            relations in the schema.
        """
        return "".join(
            ["digraph {\n"] +
            [
                f"{r.parent.name} -> {r.child.name}\n"
                for r in self.relations
            ] +
            ["}\n"]
        )


# Latest I/O schema shared definitions
_DEFS = io.SCHEMA.json['$defs']
# Checkout properties from the current I/O schema
_CHECKOUT = _DEFS['checkout']['properties']
# Build properties from the current I/O schema
_BUILD = _DEFS['build']['properties']
# Test properties from the current I/O schema
_TEST = _DEFS['test']['properties']
# Issue properties from the current I/O schema
_ISSUE = _DEFS['issue']['properties']
# Issue culprit properties from the current I/O schema
_ISSUE_CULPRIT = _ISSUE['culprit']['properties']
# Incident properties from the current I/O schema
_INCIDENT = _DEFS['incident']['properties']
# Test environment properties from the current I/O schema
_TEST_ENVIRONMENT = _TEST['environment']['properties']

# The schema of the raw object-oriented data
SCHEMA = Schema(
    _DEFS,
    dict(
        revision=dict(
            field_json_schemas=dict(
                git_commit_hash=_CHECKOUT['git_commit_hash'],
                patchset_hash=_CHECKOUT['patchset_hash'],
                patchset_files=_CHECKOUT['patchset_files'],
                git_commit_name=_CHECKOUT['git_commit_name'],
                contacts=_CHECKOUT['contacts'],
            ),
            required_fields=set(),
            id_fields=("git_commit_hash", "patchset_hash"),
            children=dict(
                checkout=("git_commit_hash", "patchset_hash",)
            ),
        ),
        checkout=dict(
            field_json_schemas=dict(
                id=_CHECKOUT['id'],
                git_commit_hash=_CHECKOUT['git_commit_hash'],
                patchset_hash=_CHECKOUT['patchset_hash'],
                origin=_CHECKOUT['origin'],
                git_repository_url=_CHECKOUT['git_repository_url'],
                git_repository_branch=_CHECKOUT['git_repository_branch'],
                tree_name=_CHECKOUT['tree_name'],
                message_id=_CHECKOUT['message_id'],
                start_time=_CHECKOUT['start_time'],
                log_url=_CHECKOUT['log_url'],
                log_excerpt=_CHECKOUT['log_excerpt'],
                comment=_CHECKOUT['comment'],
                valid=_CHECKOUT['valid'],
                misc=_CHECKOUT['misc'],
            ),
            required_fields={'id', 'origin'},
            id_fields=("id",),
            children=dict(
                build=("checkout_id",)
            ),
        ),
        build=dict(
            field_json_schemas=dict(
                id=_BUILD['id'],
                checkout_id=_BUILD['checkout_id'],
                origin=_BUILD['origin'],
                start_time=_BUILD['start_time'],
                duration=_BUILD['duration'],
                architecture=_BUILD['architecture'],
                command=_BUILD['command'],
                compiler=_BUILD['compiler'],
                input_files=_BUILD['input_files'],
                output_files=_BUILD['output_files'],
                config_name=_BUILD['config_name'],
                config_url=_BUILD['config_url'],
                log_url=_BUILD['log_url'],
                log_excerpt=_BUILD['log_excerpt'],
                comment=_BUILD['comment'],
                valid=_BUILD['valid'],
                misc=_BUILD['misc'],
            ),
            required_fields={'id', 'origin', 'checkout_id'},
            id_fields=("id",),
            children=dict(
                test=("build_id",),
                incident=("build_id",),
            ),
        ),
        test=dict(
            field_json_schemas=dict(
                id=_TEST['id'],
                build_id=_TEST['build_id'],
                origin=_TEST['origin'],
                path=_TEST['path'],
                environment_comment=_TEST_ENVIRONMENT['comment'],
                environment_misc=_TEST_ENVIRONMENT['misc'],
                status=_TEST['status'],
                waived=_TEST['waived'],
                start_time=_TEST['start_time'],
                duration=_TEST['duration'],
                output_files=_TEST['output_files'],
                log_url=_TEST['log_url'],
                log_excerpt=_TEST['log_excerpt'],
                comment=_TEST['comment'],
                misc=_TEST['misc'],
            ),
            required_fields={'id', 'origin', 'build_id'},
            id_fields=("id",),
            children=dict(
                incident=("test_id",),
            ),
        ),
        bug=dict(
            field_json_schemas=dict(
                url=_ISSUE['report_url'],
                subject=_ISSUE['report_subject'],
                culprit_code=_ISSUE_CULPRIT['code'],
                culprit_tool=_ISSUE_CULPRIT['tool'],
                culprit_harness=_ISSUE_CULPRIT['harness'],
            ),
            required_fields={'url'},
            id_fields=("url",),
            children=dict(
                issue=("report_url",),
            ),
        ),
        issue=dict(
            field_json_schemas=dict(
                id=_ISSUE['id'],
                version=_ISSUE['version'],
                origin=_ISSUE['origin'],
                report_url=_ISSUE['report_url'],
                report_subject=_ISSUE['report_subject'],
                culprit_code=_ISSUE_CULPRIT['code'],
                culprit_tool=_ISSUE_CULPRIT['tool'],
                culprit_harness=_ISSUE_CULPRIT['harness'],
                build_valid=_ISSUE['build_valid'],
                test_status=_ISSUE['test_status'],
                comment=_ISSUE['comment'],
                misc=_ISSUE['misc'],
            ),
            required_fields={'id', 'version', 'origin'},
            id_fields=("id",),
            children=dict(
                incident=("issue_id",),
            ),
        ),
        incident=dict(
            field_json_schemas=dict(
                id=_INCIDENT['id'],
                origin=_INCIDENT['origin'],
                issue_id=_INCIDENT['issue_id'],
                issue_version=_INCIDENT['issue_version'],
                build_id=_INCIDENT['build_id'],
                test_id=_INCIDENT['test_id'],
                comment=_INCIDENT['comment'],
                misc=_INCIDENT['misc'],
            ),
            required_fields={'id', 'origin', 'issue_id', 'issue_version'},
            id_fields=("id",),
        ),
    )
)

assert all(k.endswith("s") for k in io.SCHEMA.graph if k), \
    "Not all I/O object list names end with 's'"

assert set(SCHEMA.types) >= \
    set(k[:-1] for k in io.SCHEMA.graph if k), \
    "OO types are not a superset of I/O types"

# A (verbose) regular expression pattern matching an unquoted ID field
_PATTERN_STRING_ID_FIELD_UNQUOTED_PATTERN = """
    [\x30-\x39\x41-\x5a\x61-\x7a_:/.?%+-]+
"""

_PATTERN_STRING_ID_FIELD_UNQUOTED_RE = re.compile(
    _PATTERN_STRING_ID_FIELD_UNQUOTED_PATTERN,
    re.ASCII | re.VERBOSE
)

# A (verbose) regular expression pattern matching characters which can
# appear unescaped in a quoted ID field
_PATTERN_STRING_ID_FIELD_QUOTED_UNESC_CHAR_PATTERN = """
    # Anything printable except doublequote/backslash
    [\x5d-\x7e\x20-\x21\x23-\x5b]
"""

# A (verbose) regular expression pattern matching characters which must be
# backslash-escaped when appearing in a quoted ID field
_PATTERN_STRING_ID_FIELD_QUOTED_ESC_CHAR_PATTERN = """
    # Doublequote/backslash
    ["\\\\]
"""

# A (verbose) regular expression pattern matching a quoted ID field
_PATTERN_STRING_ID_FIELD_QUOTED_PATTERN = f"""
    "
        (?:
            {_PATTERN_STRING_ID_FIELD_QUOTED_UNESC_CHAR_PATTERN} |
            \\\\ {_PATTERN_STRING_ID_FIELD_QUOTED_ESC_CHAR_PATTERN}
        )*
    "
"""

# A (verbose) regular expression pattern matching an ID field
_PATTERN_STRING_ID_FIELD_PATTERN = f"""
    (?:
        {_PATTERN_STRING_ID_FIELD_UNQUOTED_PATTERN} |
        {_PATTERN_STRING_ID_FIELD_QUOTED_PATTERN}
    )
"""

# A (verbose) regular expression pattern matching an ID (ID field list)
_PATTERN_STRING_ID_PATTERN = f"""
    {_PATTERN_STRING_ID_FIELD_PATTERN}
    (?:
        \\s*
        ,
        \\s*
        {_PATTERN_STRING_ID_FIELD_PATTERN}
    )*
"""

# A (verbose) regular expression pattern matching an ID list
_PATTERN_STRING_ID_LIST_PATTERN = f"""
    {_PATTERN_STRING_ID_PATTERN}
    (?:
        \\s*
        ;
        \\s*
        {_PATTERN_STRING_ID_PATTERN}
    )*
"""

# A (verbose) regular expression pattern matching an ID list spec
_PATTERN_STRING_SPEC_ID_LIST_PATTERN = f"""
    \\[
        \\s*
        (?:
            {_PATTERN_STRING_ID_LIST_PATTERN}
            \\s*
        )?
    \\]
"""

# A (verbose) regular expression pattern matching a spec
_PATTERN_STRING_SPEC_PATTERN = f"""
    (?:
        # ID list placeholder
        % |
        # Inline ID list
        {_PATTERN_STRING_SPEC_ID_LIST_PATTERN}
    )
"""

# A (verbose) regular expression pattern matching a "pattern" part of the
# pattern string. Matching group names correspond to component ABNF rules.
_PATTERN_STRING_PATTERN = f"""
    \\s*
    # Relation
    (?P<relation>
        [<>]
    )
    \\s*
    # Type
    (?P<type>
        # Type name
        [a-z0-9_]+ |
        # Type branch wildcard
        [*]
    )
    \\s*
    # ID list specification
    (?P<spec>
        {_PATTERN_STRING_SPEC_PATTERN}
    )?
    \\s*
    # Matching scope
    (?P<match>
        [#$]
    )?
    \\s*
"""

# A regular expression matching a "pattern" part of the pattern string
# Matching group names correspond to component ABNF rules.
_PATTERN_STRING_RE = re.compile(
    _PATTERN_STRING_PATTERN,
    re.ASCII | re.VERBOSE
)


# TODO: Make Pattern immutable
class Pattern:
    """A pattern matching objects in a data source"""

    # No, it's OK, pylint: disable=too-many-arguments
    def __init__(self, base, child, obj_type, obj_id_set=None):
        """
        Initialize an object pattern.

        Args:
            base:           The pattern for objects this pattern is to be
                            based on, or None, meaning the pattern is based on
                            the root object.
            child:          True if this is a pattern for a base's child type,
                            False if it's for a parent type.
            obj_type:       The type (kcidb.orm.Type) of objects referred
                            to by this pattern.
            obj_id_set:     The set/frozenset of IDs of the objects to limit
                            the pattern to, or None to not limit the objects
                            by IDs. Default is None. Sets will be converted to
                            frozensets. Each ID is a tuple of strings with the
                            number of elements corresponding to the number of
                            ID fields of the object type.
        """
        assert base is None or isinstance(base, Pattern)
        assert isinstance(child, bool)
        assert isinstance(obj_type, Type)
        obj_id_fields = obj_type.id_fields
        assert obj_id_set is None or \
               isinstance(obj_id_set, (set, frozenset)) and \
               all(
                    isinstance(obj_id, tuple) and
                    len(obj_id) == len(obj_id_fields) and
                    all(isinstance(part, (str, type(None))) for part in obj_id)
                    for obj_id in obj_id_set
               )
        assert base is not None or child
        assert base is None or (
            (
                child and
                obj_type.name in base.obj_type.children
            ) or
            (
                not child and
                base.obj_type.name in obj_type.children
            )
        )

        self.base = base
        self.child = child
        self.obj_type = obj_type
        self.obj_id_set = \
            None if obj_id_set is None else frozenset(obj_id_set)
        self._hash = hash(
            (self.base, self.child, self.obj_type, self.obj_id_set)
        )

    def __eq__(self, other):
        return \
            isinstance(other, Pattern) and \
            self.base == other.base and \
            self.child == other.child and \
            self.obj_type == other.obj_type and \
            self.obj_id_set == other.obj_id_set

    def __hash__(self):
        return self._hash

    @staticmethod
    def _format_id_field(id_field):
        """
        Format an ID field for a string representation of a pattern.

        Args:
            id_field:   The ID field to format.

        Returns:
            The formatted (quoted or unquoted) ID field.
        """
        # If we can leave the field unquoted
        if re.fullmatch(_PATTERN_STRING_ID_FIELD_UNQUOTED_PATTERN,
                        id_field, re.ASCII | re.VERBOSE):
            return id_field
        part_re = re.compile(f"""
            ({_PATTERN_STRING_ID_FIELD_QUOTED_ESC_CHAR_PATTERN}) |
            ({_PATTERN_STRING_ID_FIELD_QUOTED_UNESC_CHAR_PATTERN}*)
        """, re.ASCII | re.VERBOSE)
        parts = []
        pos = 0
        while True:
            match = part_re.match(id_field, pos)
            esc, unesc = match.group(1, 2)
            pos = match.end()
            if unesc:
                parts.append(unesc)
            elif esc:
                parts += ["\\", esc]
            elif pos < len(id_field):
                raise Exception(
                    f"ID field cannot be represented in a pattern string: "
                    f"{id_field}"
                )
            else:
                break
        return '"' + "".join(parts) + '"'

    @staticmethod
    def _format_id_list_spec(obj_id_set):
        """
        Format an ID list spec for a string representation of a pattern.

        Args:
            obj_id_set: The set of IDs to format as a spec,
                        or None if missing.

        Returns:
            The string representation of the ID list spec,
            or empty string if not specified.
        """
        if obj_id_set is None:
            return ""
        return "[" + "; ".join(
            ", ".join(
                Pattern._format_id_field(obj_id_field)
                for obj_id_field in obj_id
            )
            for obj_id in obj_id_set
        ) + "]"

    def __repr__(self, final=True):
        string = ""
        if self.base is not None:
            string += self.base.__repr__(final=False)
        string += ">" if self.child else "<"
        string += self.obj_type.name
        string += Pattern._format_id_list_spec(self.obj_id_set)
        if final:
            string += "#"
        return string

    @staticmethod
    def _validate_obj_id_set(obj_type, obj_id_set):
        """
        Validate a set/frozenset of object IDs against a type.

        Args:
            obj_type:   The type to validate the ID set/frozenset against.
            obj_id_set: The ID set/frozenset to validate, or None for no IDs.

        Returns:
            The validated ID set/frozenset, or None if supplied with None.

        Raises:
            Exception if the ID set/frozenset is invalid for the type.
        """
        assert isinstance(obj_type, Type)
        assert obj_id_set is None or \
            (isinstance(obj_id_set, (set, frozenset)) and
             all(isinstance(obj_id, tuple) and
                 all(isinstance(part, (str, type(None))) for part in obj_id)
                 for obj_id in obj_id_set))
        for obj_id in obj_id_set or []:
            if len(obj_id) != len(obj_type.id_fields):
                raise Exception(
                    f"Invalid number of ID fields "
                    f"({len(obj_id)}) for {obj_type.name!r}. "
                    f"Expecting {len(obj_type.id_fields)} "
                    f"fields: {obj_id!r}"
                )
        return obj_id_set

    @staticmethod
    def _expand_relation(schema, base_set,
                         child, obj_type_expr, obj_id_set):
        """
        Expand a single level of parent/child relation into a list of
        patterns, for a parsed pattern specification.

        Args:
            schema:         An object type schema to use.
            base_set:       The set of patterns to base created patterns on.
                            Empty set means patterns shouldn't be based on
                            anything (based on the "root" type).
            child:          True, if expanding children of the specified
                            bases. False, if parents.
            obj_type_expr:  Object type expression, one of:
                            "*" - all children types,
                            or a name of the specific child type.
            obj_id_set:     The set/frozenset of object IDs to limit the
                            pattern to, or None to not limit the pattern.

        Returns:
            Two values: a set of new patterns expanded from the
            specification, referencing the supplied bases, and a set of
            unused bases (having no relations to substitute for "*").
        """
        assert isinstance(schema, Schema)
        assert isinstance(base_set, set)
        assert all(isinstance(base, Pattern) for base in base_set)
        assert isinstance(child, bool)
        assert isinstance(obj_type_expr, str)
        assert obj_id_set is None or \
            (isinstance(obj_id_set, (set, frozenset)) and
             all(isinstance(obj_id, tuple) and
                 all(isinstance(part, (str, type(None))) for part in obj_id)
                 for obj_id in obj_id_set))

        new_set = set()
        unused_set = set()
        # If we are based on some objects
        if base_set:
            # For each base
            for base in base_set:
                base_new_set = set()
                # For each base's relation of requested type
                related_types = (
                    relation.child if child else relation.parent
                    for relation in (base.obj_type.children if child
                                     else base.obj_type.parents).values()
                )
                for obj_type in related_types:
                    if obj_type_expr in ("*", obj_type.name):
                        base_new_set.add(
                            Pattern(
                                base, child, obj_type,
                                Pattern._validate_obj_id_set(obj_type,
                                                             obj_id_set)
                            )
                        )
                # If we have expanded to something
                if base_new_set:
                    new_set |= base_new_set
                else:
                    if obj_type_expr == "*":
                        unused_set.add(base)
                    else:
                        raise Exception(
                            f"Cannot find {'child' if child else 'parent'} "
                            f"type {obj_type_expr!r}"
                        )
        # Else we're based on "root", and if children were requested
        elif child:
            for obj_type_name, obj_type in schema.types.items():
                if obj_type_expr in ("*", obj_type_name):
                    new_set.add(
                        Pattern(
                            None, child, obj_type,
                            Pattern._validate_obj_id_set(obj_type,
                                                         obj_id_set)
                        )
                    )
            # If we have expanded to nothing
            if not new_set and obj_type_expr != "*":
                raise Exception(
                    f"Cannot find type {obj_type_expr!r}"
                )
        return new_set, unused_set

    @staticmethod
    def _expand(schema, base_set, match_set, child, obj_type_expr,
                obj_id_set, match_spec):
        """
        Expand a parsed pattern specification into a list of referenced
        patterns, and a list of matching patterns.

        Args:
            schema:         An object type schema to use.
            base_set:       The set of patterns to base created patterns
                            on. Empty set means patterns shouldn't be
                            based on anything (based on the "root" object).
            match_set:      The set to have matching patterns added to,
                            if any.
            child:          True if the created patterns are for children
                            of the specified bases. False for parents.
            obj_type_expr:  Object type expression, one of:
                            "*" - all children/parents,
                            or a name of the specific type.
            obj_id_set:     The set/frozenset of object IDs to limit the
                            pattern to, or None to not limit the pattern.
            match_spec:     The matching specification string ("#", or "$"),
                            or None, if expanded patterns shouldn't be marked
                            for matching.

        Returns:
            The set of patterns referenced by the specification.
        """
        assert isinstance(base_set, set)
        assert all(isinstance(base, Pattern) for base in base_set)
        assert isinstance(match_set, set)
        assert isinstance(child, bool)
        assert isinstance(obj_type_expr, str)
        assert obj_id_set is None or \
            (isinstance(obj_id_set, (set, frozenset)) and
             all(isinstance(obj_id, tuple) and
                 all(isinstance(part, (str, type(None))) for part in obj_id)
                 for obj_id in obj_id_set))
        assert match_spec in (None, "#", "$")

        ref_set = set()
        while True:
            base_set, unused_set = Pattern._expand_relation(
                schema, base_set, child, obj_type_expr, obj_id_set)
            if obj_type_expr == "*":
                ref_set |= unused_set
                if match_spec == "$":
                    match_set |= unused_set
                if not base_set:
                    break
                if match_spec == "#":
                    match_set |= base_set
            else:
                ref_set |= base_set
                if match_spec is not None:
                    match_set |= base_set
                break

        return ref_set

    @staticmethod
    def _parse_id(string, pos):
        """
        Parse an ID string from a pattern.

        Args:
            string: The ID string to parse.
                    Must start with a string formatted according to "id" ABNF
                    rule, possibly followed by whitespace, and optionally
                    anything but a comma.
            pos:    The position to start parsing at.

        Returns:
            A tuple containing parsed ID fields, and the stopping position at
            the end of whitespace following the ID.
        """
        assert LIGHT_ASSERTS or re.match(_PATTERN_STRING_ID_PATTERN,
                                         string[pos:],
                                         re.ASCII | re.VERBOSE)
        id_fields = []

        # NOTE: Not handling failures here, we're expecting a valid string
        while True:
            # If it's a quoted field
            if string[pos] == '"':
                pos += 1
                id_field_chars = []
                while True:
                    char = string[pos]
                    pos += 1
                    if char == '"':
                        break
                    if char == '\\':
                        char = string[pos]
                        pos += 1
                    id_field_chars.append(char)
                id_field = "".join(id_field_chars)
            # Else it's an unquoted field
            else:
                match = _PATTERN_STRING_ID_FIELD_UNQUOTED_RE.match(
                            string, pos)
                pos = match.end()
                id_field = match.group(0)
            # Add the field to the ID
            id_fields.append(id_field)
            # Skip whitespace
            while pos < len(string) and string[pos].isspace():
                pos += 1
            # If it's not an ID field separator
            if pos >= len(string) or string[pos] != ',':
                break
            # Skip ID field separator
            pos += 1
            # Skip whitespace
            while string[pos].isspace():
                pos += 1

        return tuple(id_fields), pos

    @staticmethod
    def _parse_id_list(string):
        """
        Parse an ID list.

        Args:
            string: The ID list string to parse.
                    Must be formatted according to "id_list" ABNF rule.

        Returns:
            A frozenset containing ID field tuples parsed from the string.
        """
        assert LIGHT_ASSERTS or re.fullmatch(_PATTERN_STRING_ID_LIST_PATTERN,
                                             string, re.ASCII | re.VERBOSE)
        # NOTE: Not handling failures here, we're expecting a valid string
        id_list = []
        pos = 0
        while True:
            # We like our "id", pylint: disable=invalid-name
            # Parse next ID
            id, pos = Pattern._parse_id(string, pos)
            id_list.append(id)
            # Stop, if we ran out of IDs
            if pos >= len(string) or string[pos] != ';':
                break
            # Skip ID separator
            pos += 1
            # Skip whitespace
            while string[pos].isspace():
                pos += 1
        return frozenset(id_list)

    @staticmethod
    def _parse_spec(string, obj_id_set_list):
        """
        Parse an optional ID list specification from a pattern string,
        possibly consuming an element from the supplied object ID list list.

        Args:
            string:             The ID list specification string to parse,
                                or None, meaning the pattern had no ID list
                                specification.
            obj_id_set_list:    The list of object ID sets/frozensets to
                                retrieve IDs for placeholders from, or None,
                                meaning no list was supplied with the pattern
                                string. Not modified.

        Returns:
            Two items:
                * the retrieved object ID set/frozenset, or None, if no
                  specification was provided with the pattern;
                * the provided list of object ID sets/frozensets, possibly
                  with the first element removed, if it was consumed by a
                  placeholder.

        Raises:
            Exception with a message, if there were no, or not enough ID
            sets/frozensets, when required.
        """
        assert string is None or (
            isinstance(string, str) and (
                LIGHT_ASSERTS or
                re.fullmatch(_PATTERN_STRING_SPEC_PATTERN,
                             string, re.ASCII | re.VERBOSE)
            )
        )

        # NOTE: Not handling failures here, we're expecting a valid string
        if string is None:
            return None, obj_id_set_list
        if string == "%":
            if obj_id_set_list is None:
                raise Exception(
                    "No ID list list specified to substitute the placeholder"
                )
            try:
                return obj_id_set_list[0], obj_id_set_list[1:]
            except IndexError:
                raise Exception("Not enough ID lists specified") from None
        # Parse the ID list inside brackets, if any
        brackets_contents = string[1:-1].strip()
        return (Pattern._parse_id_list(brackets_contents)
                if brackets_contents else frozenset()), obj_id_set_list

    STRING_DOC = textwrap.dedent("""\
        The pattern string is a series of pattern specifications, each
        consisting of a relation character, followed by object type
        specification, followed by the optional ID list specification,
        followed by the optional matching specification. It could be described
        using ABNF:

        whitespace = %x09-0d / %x20 ; Whitespace characters
        relation = ">" /    ; Traverse children of the types referenced by
                            ; the pattern on the left, or of the "root type",
                            ; if there is no pattern on the left.
                   "<"      ; Traverse parents of the types referenced by the
                            ; pattern on the left.
        type = name /       ; Traverse and reference the immediate
                            ; parent/child type with specified name.
               "*"          ; Traverse all parents/children recursively.
                            ; Reference the furthest traversed type, and the
                            ; bases (types referenced by the pattern on the
                            ; left), which have no specified relations.
        name_char = %x30-39 / %x61-7a / "_"
                            ; Lower-case letters, numbers, underscore
        name = 1*name_char  ; Type name
        id_field_unquoted_char = %x30-39 / %x41-5a / %x61-7a /
                                 "_" / ":" / "/" / "." / "?" / "%" / "+" / "-"
                            ; Characters permitted in unquoted ID fields:
                            ; letters, numbers, misc characters
        id_field_quoted_token = (%x20-21 / %x23-5b / %x5d-7e) /
                                "\\" (%x22 / %x5c)
                            ; Character sequences allowed in quoted ID fields:
                            ; anything printable except backslash or
                            ; doublequote, or backslash-escaped
                            ; backslash/doublequote.
        id_field = 1*id_field_unquoted_char /
                   %x22 *id_field_quoted_token %x22
                            ; Quoted/unquoted ID field
        id = id_field *(*whitespace "," *whitespace id)
                            ; ID (a sequence of ID fields)
        id_list = id *(*whitespace ";" *whitespace id_list)
                            ; A list of IDs
        spec = "%" /        ; ID list placeholder.
                            ; Consumes one ID list from the
                            ; separately-supplied list of ID lists to limit
                            ; objects of the types traversed by this pattern
                            ; specification. Each object type gets the same ID
                            ; list. Not allowed, if the list of ID lists isn't
                            ; supplied.
               "[" *whitespace [id_list *whitespace] "]"
                            ; Inline ID list
        match = "#" /       ; Match objects of all types traversed by this
                            ; pattern specification.
                "$"         ; Match objects referenced by this pattern
                            ; specification.
        pattern = *whitespace relation *whitespace type
                  [*whitespace spec] [*whitespace match]
        pattern_string = 1*pattern *whitespace

        Examples:
            >build%#            Match builds with IDs from the first item of a
                                separately-specified list of ID lists (if
                                supplied).
            >build%$            The same.
            >build[redhat:1077837]#
                                Match a build with ID "redhat:1077837".
            >checkout%>build#   Match builds of checkouts with IDs from
                                the first element of separately-specified list
                                of ID lists (if supplied).
            >test%<build#       Match builds of tests with IDs from the first
                                element of separately-specified list of ID
                                lists (if supplied).
            >test[redhat:1077834_0; redhat:1077834_1]<build#
                                Match builds of tests with IDs
                                "redhat:1077834_0" and "redhat:1077834_1".
            >test%<*#           Match all parents of tests with IDs from the
                                first element of separately-specified list of
                                ID lists (if supplied), but not the tests
                                themselves.
            >test%<*$           Match only the furthest (the ultimate) parents
                                of tests with IDs from the optional ID list
                                list, including tests themselves, if they have
                                no parent types.
            >revision%#>*#      Match revisions with IDs from the optional ID
                                list list, and all their children, if any.
            >revision[c763deac7ff, 932e2d61add]#>*#
                                Match the revision with ID (c763deac7ff,
                                932e2d61add), and all its children, if any.
            >test%<*$>*#        Match the root objects containing tests with
                                the IDs from the optional ID list list, along
                                with all their children.
            >*#                 Match everything in the database.
            >*$                 Match objects of all childless types.
    """)

    @staticmethod
    def parse(string, obj_id_set_list=None, schema=None):
        """
        Parse a pattern string and its parameter IDs into a tree of Pattern
        objects. See kcidb.orm.Pattern.STRING_DOC for documentation on
        pattern strings.

        Args:
            string:             The pattern string.
            obj_id_set_list:    A list of ID sets/frozensets to use to filter
                                the referenced objects with, in the order
                                specified in the pattern string. Each ID is a
                                tuple with ID column value strings. If not
                                specified, or specified as None, ID list
                                placeholders are not allowed in the pattern
                                string.
            schema:             An object type schema to use, or None to use
                                kcidb.orm.SCHEMA.

        Returns:
            A set of leaf Pattern objects selecting the ORM objects matched by
            the pattern string.
        """
        assert isinstance(string, str)
        assert obj_id_set_list is None or (
            isinstance(obj_id_set_list, list) and
            all(
                isinstance(obj_id_set, (set, frozenset)) and
                all(isinstance(obj_id, tuple) for obj_id in obj_id_set)
                for obj_id_set in obj_id_set_list
            )
        )
        assert schema is None or isinstance(schema, Schema)
        if schema is None:
            schema = SCHEMA

        base_set = set()
        match_set = set()
        pos = 0
        while pos < len(string):
            match = _PATTERN_STRING_RE.match(string, pos)
            if not match:
                raise Exception(f"Invalid pattern string {string!r} "
                                f"at position {pos}: {string[pos:]!r}")
            relation, obj_type_expr, spec, match_spec = \
                match.group("relation", "type", "spec", "match")
            obj_id_set, obj_id_set_list = Pattern._parse_spec(
                spec, obj_id_set_list
            )
            try:
                base_set = Pattern._expand(
                    schema, base_set, match_set, relation == ">",
                    obj_type_expr, obj_id_set, match_spec
                )
            except Exception as exc:
                raise Exception(
                    f"Failed expanding pattern specification "
                    f"at position {pos}: {string[pos:]!r}"
                ) from exc
            pos = match.end()
        if obj_id_set_list:
            raise Exception(
                f"Too many ID sets specified for pattern {string!r}"
            )
        return match_set

    @staticmethod
    def from_io(io_data, schema=None, max_objs=0):
        """
        Create a pattern set matching all objects in the supplied I/O data.

        Args:
            io_data:    The I/O data to create the pattern list from.
                        Must adhere to the current schema version.
            schema:     An object type schema to use, or None to use
                        kcidb.orm.SCHEMA.
            max_objs:   Maximum number of object IDs to put into each
                        created pattern (a positive integer).
                        Zero for no limit.

        Returns:
            A set of Pattern objects matching the objects in the supplied I/O
            data.
        """
        assert io.SCHEMA.is_compatible_exactly(io_data)
        assert LIGHT_ASSERTS or io.SCHEMA.is_valid_exactly(io_data)
        assert schema is None or isinstance(schema, Schema)
        assert isinstance(max_objs, int) and max_objs >= 0

        if schema is None:
            schema = SCHEMA
        # Assert all I/O object lists are represented in the OO schema
        assert set(schema.types) >= \
            set(k[:-1] for k in io.SCHEMA.graph if k), \
            "Specified OO types are not a superset of I/O types"
        pattern_set = set()
        for obj_list_name in io.SCHEMA.graph:
            if not obj_list_name:
                continue
            assert obj_list_name.endswith("s")
            obj_list = io_data.get(obj_list_name, [])
            if not obj_list:
                continue
            obj_type = schema.types[obj_list_name[:-1]]
            id_fields = obj_type.id_fields
            for obj_list_slice in kcidb.misc.isliced(obj_list, max_objs):
                pattern_set.add(
                    Pattern(None, True, obj_type, {
                        tuple(o[id_field] for id_field in id_fields)
                        for o in obj_list_slice
                    })
                )
        return pattern_set


class Source(ABC):
    """An abstract source of raw object-oriented (OO) data"""

    @abstractmethod
    def oo_query(self, pattern_set):
        """
        Retrieve raw data for objects specified via a pattern set.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            raw data of the corresponding type.
        """
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, Pattern) for r in pattern_set)


class Prefetcher(Source):
    """A prefetching source of object-oriented data"""

    def __init__(self, source):
        """
        Initialize the prefetching source.

        Args:
            source: The source to request objects from.
        """
        assert isinstance(source, Source)
        self.source = source

    def oo_query(self, pattern_set):
        """
        Retrieve raw data for objects specified via a pattern set.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            raw data of the corresponding type.
        """
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, Pattern) for r in pattern_set)
        # First fetch the data we were asked for
        data = self.source.oo_query(pattern_set)
        # Generate patterns for all children of fetched root objects
        prefetch_pattern_set = set()
        for obj_type_name, objs in data.items():
            obj_type = SCHEMA.types[obj_type_name]
            if not obj_type.parents and objs:
                # TODO Get rid of formatting and parsing
                # It isn't,
                # pylint: disable=bad-option-value,unnecessary-dunder-call
                prefetch_pattern_set |= Pattern.parse(
                    Pattern(None, True, obj_type,
                            {obj_type.get_id(obj) for obj in objs}).
                    __repr__(final=False) + ">*#"
                )
        # Prefetch, if generated any patterns
        if prefetch_pattern_set:
            LOGGER.info("Prefetching %r", prefetch_pattern_set)
            self.source.oo_query(prefetch_pattern_set)

        # Return the data for the original request
        return data


class Cache(Source):
    """A cache source of object-oriented data"""

    def __init__(self, source):
        """
        Initialize the cache source.

        Args:
            source: The source to request uncached objects from.
        """
        assert isinstance(source, Source)
        self.source = source
        self.reset()

    def reset(self):
        """
        Reset the cache.
        """
        self.type_id_objs = {type_name: {} for type_name in SCHEMA.types}
        self.pattern_responses = {}

    def _merge_pattern_response(self, pattern, response):
        """
        Process a response to a single-pattern query fetched from the
        underlying source, merging it with the cache.

        Args:
            pattern:    The pattern the response was retrieved for.
            response:   The retrieved response. May be modified.

        Returns:
            The merged response.
        """
        # Let's get it working first, refactor later,
        # pylint: disable=too-many-locals,too-many-branches
        assert isinstance(pattern, Pattern)
        assert LIGHT_ASSERTS or SCHEMA.is_valid(response)
        assert len(response) <= 1
        if not response:
            response[pattern.obj_type.name] = []
        type_name, objs = tuple(response.items())[0]
        assert type_name == pattern.obj_type.name

        # Merge the response and the cache
        get_id = SCHEMA.types[type_name].get_id
        get_parent_id = SCHEMA.types[type_name].get_parent_id
        id_objs = self.type_id_objs[type_name]
        base_pattern = pattern.base
        base_type = None if base_pattern is None else base_pattern.obj_type
        cached = set()
        # For each object in the response
        for obj in objs:
            # Deduplicate or cache the object
            # We like our "id", pylint: disable=invalid-name
            id = get_id(obj)
            if id in id_objs:
                obj = id_objs[id]
                LOGGER.debug("Deduplicated %r %r", type_name, id)
            else:
                id_objs[id] = obj
                LOGGER.debug("Cached %r %r", type_name, id)
            # If we've got all children of an object
            if base_pattern is not None and \
               pattern.child and pattern.obj_id_set is None:
                # Put the fact in the cache
                parent_child_pattern = Pattern(
                    Pattern(None, True, base_type,
                            {get_parent_id(base_type.name, obj)}),
                    True, pattern.obj_type
                )
                if parent_child_pattern in cached:
                    r = self.pattern_responses[parent_child_pattern]
                    r[type_name].append(obj)
                elif parent_child_pattern not in self.pattern_responses:
                    self.pattern_responses[parent_child_pattern] = {
                        type_name: [obj]
                    }
                    cached.add(parent_child_pattern)

        # If we've just loaded all children of the parent pattern
        if pattern.child and base_pattern in self.pattern_responses and \
           pattern.obj_id_set is None:
            parent_get_id = SCHEMA.types[base_type.name].get_id
            parent_response = self.pattern_responses[base_pattern]
            # For each cached parent object
            for parent_obj in parent_response[base_type.name]:
                # Create pattern for its children
                parent_child_pattern = Pattern(
                    Pattern(None, True, base_type,
                            {parent_get_id(parent_obj)}),
                    True, pattern.obj_type
                )
                # If we don't have its children cached
                if parent_child_pattern not in self.pattern_responses:
                    # Store the fact that it has none
                    self.pattern_responses[parent_child_pattern] = {
                        type_name: []
                    }
                    cached.add(parent_child_pattern)

        # For every parent-child relation of this pattern's type
        for child_relation in pattern.obj_type.children.values():
            # If we had a query for all this-type children of our pattern
            sub_pattern = Pattern(pattern, True, child_relation.child)
            if sub_pattern in self.pattern_responses:
                # For each object in our response
                for obj in objs:
                    # Create pattern for its children of this type
                    parent_child_pattern = Pattern(
                        Pattern(None, True, pattern.obj_type, {get_id(obj)}),
                        True, child_relation.child
                    )
                    # If we don't have its children cached
                    if parent_child_pattern not in self.pattern_responses:
                        # Store the fact that it has none
                        self.pattern_responses[parent_child_pattern] = {
                            child_relation.child.name: []
                        }
                        cached.add(parent_child_pattern)

        # Add pattern response to the cache
        self.pattern_responses[pattern] = response
        cached.add(pattern)
        # Log cached patterns
        LOGGER.debug("Cached patterns %r", cached)
        if LOGGER.getEffectiveLevel() <= logging.INFO:
            LOGGER.info(
                "Cache has %s",
                ", ".join(
                    tuple(
                        f"{len(id_objs)} {type_name}s"
                        for type_name, id_objs in self.type_id_objs.items()
                        if id_objs
                    ) +
                    (f"{len(self.pattern_responses)} patterns", )
                )
            )
        return response

    def oo_query(self, pattern_set):
        """
        Retrieve raw data for objects specified via a pattern set.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            raw data of the corresponding type.
        """
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, Pattern) for r in pattern_set)

        # Start with an empty response
        response_type_id_objs = {}

        # For each pattern
        for pattern in pattern_set:
            # Try to get the response from the cache
            try:
                pattern_response = self.pattern_responses[pattern]
                LOGGER.debug("Fetched from the cache: %r", pattern)
            # If not found
            except KeyError:
                # Query the source and merge the response into the cache
                pattern_response = self._merge_pattern_response(
                    pattern, self.source.oo_query({pattern})
                )
                LOGGER.debug("Merged into the cache: %r", pattern)
            # Merge into the overall response
            for type_name, objs in pattern_response.items():
                get_id = SCHEMA.types[type_name].get_id
                id_objs = response_type_id_objs.get(type_name, {})
                for obj in objs:
                    id_objs[get_id(obj)] = obj
                response_type_id_objs[type_name] = id_objs

        # Return merged and validated response
        response = {
            type_name: list(id_objs.values())
            for type_name, id_objs in response_type_id_objs.items()
        }
        assert LIGHT_ASSERTS or SCHEMA.is_valid(response)
        return response


class PatternHelpAction(argparse.Action):
    """Argparse action outputting pattern string help and exiting."""
    def __init__(self,
                 option_strings,
                 dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS,
                 help=None):
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        print(
            Pattern.STRING_DOC +
            "\n" +
            "NOTE: Specifying object ID lists separately is not "
            "supported using\n"
            "      command-line tools. "
            "Only inline ID lists are supported.\n"
        )
        parser.exit()


def argparse_add_args(parser):
    """
    Add common ORM arguments to an argument parser.

    Args:
        The parser to add arguments to.
    """
    parser.add_argument(
        'pattern_strings',
        nargs='*',
        default=[],
        metavar='PATTERN',
        help='Object-matching pattern. '
             'See pattern documentation with --pattern-help.'
    )
    parser.add_argument(
        '--pattern-help',
        action=PatternHelpAction,
        help='Print pattern string documentation and exit.'
    )


class ArgumentParser(kcidb.misc.ArgumentParser):
    """
    Command-line argument parser with common ORM arguments added.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding common ORM arguments.

        Args:
            args:   Positional arguments to initialize ArgumentParser with.
            kwargs: Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        argparse_add_args(self)


class OutputArgumentParser(kcidb.misc.OutputArgumentParser):
    """
    Command-line argument parser for tools outputting JSON,
    with common ORM arguments added.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser, adding JSON output arguments.

        Args:
            args:   Positional arguments to initialize ArgumentParser with.
            kwargs: Keyword arguments to initialize ArgumentParser with.
        """
        super().__init__(*args, **kwargs)
        argparse_add_args(self)
