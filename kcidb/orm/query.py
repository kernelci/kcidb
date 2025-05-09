"""
The query classes and functions used by the KCIDB object-relational
mapping system to retrieve Kernel CI report data from the database.
"""

import re
import argparse
import textwrap

from kcidb.orm.data import Schema, SCHEMA, Type
import kcidb.io as io
import kcidb.misc
from kcidb.misc import LIGHT_ASSERTS

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
            obj_type:       The type (kcidb.orm.data.Type) of objects referred
                            to by this pattern.
            obj_id_set:     The set/frozenset of IDs of the objects to limit
                            the pattern to, or None to not limit the objects
                            by IDs. Default is None. Sets will be converted to
                            frozensets. Each ID is a tuple of values with the
                            types and number of elements corresponding to the
                            ID field types of the object type.
        """
        assert base is None or isinstance(base, Pattern)
        assert isinstance(child, bool)
        assert isinstance(obj_type, Type)
        obj_id_field_types = obj_type.id_field_types
        assert obj_id_set is None or \
               isinstance(obj_id_set, (set, frozenset)) and \
               all(
                    isinstance(obj_id, tuple) and
                    len(obj_id) == len(obj_id_field_types) and
                    all(isinstance(part, (part_type, type(None)))
                        for part_type, part in
                        zip(obj_id_field_types.values(), obj_id))
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
        id_field_str = str(id_field)
        # If we can leave the field unquoted
        if re.fullmatch(_PATTERN_STRING_ID_FIELD_UNQUOTED_PATTERN,
                        id_field_str, re.ASCII | re.VERBOSE):
            return id_field_str
        part_re = re.compile(f"""
            ({_PATTERN_STRING_ID_FIELD_QUOTED_ESC_CHAR_PATTERN}) |
            ({_PATTERN_STRING_ID_FIELD_QUOTED_UNESC_CHAR_PATTERN}*)
        """, re.ASCII | re.VERBOSE)
        parts = []
        pos = 0
        while True:
            match = part_re.match(id_field_str, pos)
            esc, unesc = match.group(1, 2)
            pos = match.end()
            if unesc:
                parts.append(unesc)
            elif esc:
                parts += ["\\", esc]
            elif pos < len(id_field_str):
                raise Exception(
                    f"ID field cannot be represented in a pattern string: "
                    f"{id_field!r}"
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
    def _parse_obj_str_id_set(obj_type, obj_str_id_set):
        """
        Parse a set/frozenset of string IDs for the specified object type.

        Args:
            obj_type:       The type to parse the string ID set/frozenset for.
            obj_str_id_set: The string ID set/frozenset to parse,
                            or None for no IDs.

        Returns:
            The parsed ID set/frozenset, or None if supplied with None.

        Raises:
            Exception if the string ID set/frozenset is invalid for the type.
        """
        assert isinstance(obj_type, Type)
        assert obj_str_id_set is None or \
            (isinstance(obj_str_id_set, (set, frozenset)) and
             all(isinstance(obj_str_id, tuple) and
                 all(isinstance(str_part, (str, type(None)))
                     for str_part in obj_str_id)
                 for obj_str_id in obj_str_id_set))
        if obj_str_id_set is None:
            return None
        obj_id_set = set()
        for obj_str_id in obj_str_id_set or []:
            if len(obj_str_id) != len(obj_type.id_field_types):
                raise Exception(
                    f"Invalid number of ID fields "
                    f"({len(obj_str_id)}) for {obj_type.name!r}. "
                    f"Expecting {len(obj_type.id_field_types)} "
                    f"fields: {obj_str_id!r}"
                )
            obj_id = tuple()
            for part_name, part_type, str_part in zip(
                obj_type.id_field_types.keys(),
                obj_type.id_field_types.values(),
                obj_str_id
            ):
                try:
                    obj_id += (part_type(str_part), )
                except ValueError as exc:
                    raise Exception(
                        f"Invalid {part_name!r} part {str_part!r} of "
                        f"string ID fields for {obj_type.name!r}"
                    ) from exc
            obj_id_set.add(obj_id)
        return obj_id_set

    @staticmethod
    def _expand_relation(schema, base_set,
                         child, obj_type_expr, obj_str_id_set):
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
                            "*" - all child/parent types,
                            or a name of the specific child/parent type.
            obj_str_id_set: The set/frozenset of object string IDs to limit
                            the pattern to, or None to not limit the pattern.

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
        assert obj_str_id_set is None or \
            (isinstance(obj_str_id_set, (set, frozenset)) and
             all(isinstance(obj_str_id, tuple) and
                 all(isinstance(str_part, (str, type(None)))
                     for str_part in obj_str_id)
                 for obj_str_id in obj_str_id_set))

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
                                Pattern._parse_obj_str_id_set(obj_type,
                                                              obj_str_id_set)
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
                            Pattern._parse_obj_str_id_set(obj_type,
                                                          obj_str_id_set)
                        )
                    )
            # If we have expanded to nothing
            if not new_set and obj_type_expr != "*":
                raise Exception(
                    f"Cannot find type {obj_type_expr!r}"
                )
        return new_set, unused_set

    # It's OK for now, pylint: disable=too-many-positional-arguments
    @staticmethod
    def _expand(schema, base_set, match_set, child, obj_type_expr,
                obj_str_id_set, match_spec):
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
            obj_str_id_set: The set/frozenset of object string IDs to limit
                            the pattern to, or None to not limit the pattern.
            match_spec:     The matching specification string ("#", or "$"),
                            or None, if expanded patterns shouldn't be marked
                            for matching.

        Returns:
            The set of patterns referenced by the specification.
        """
        assert isinstance(schema, Schema)
        assert isinstance(base_set, set)
        assert all(isinstance(base, Pattern) for base in base_set)
        assert isinstance(match_set, set)
        assert isinstance(child, bool)
        assert isinstance(obj_type_expr, str)
        assert obj_str_id_set is None or \
            (isinstance(obj_str_id_set, (set, frozenset)) and
             all(isinstance(obj_str_id, tuple) and
                 all(isinstance(str_part, (str, type(None)))
                     for str_part in obj_str_id)
                 for obj_str_id in obj_str_id_set))
        assert match_spec in (None, "#", "$")

        ref_set = set()
        while True:
            base_set, unused_set = Pattern._expand_relation(
                schema, base_set, child, obj_type_expr, obj_str_id_set)
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
            A string ID - tuple containing parsed string ID parts, and the
            stopping position at the end of whitespace following the ID.
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
    def _parse_spec(string, obj_str_id_set_list):
        """
        Parse an optional ID list specification from a pattern string,
        possibly consuming an element from the supplied object string ID set
        list.

        Args:
            string:                 The ID list specification string to parse,
                                    or None, meaning the pattern had no ID
                                    list specification.
            obj_str_id_set_list:    The list of object string ID
                                    sets/frozensets to retrieve string IDs for
                                    placeholders from, or None, meaning no
                                    list was supplied with the pattern string.
                                    Not modified.

        Returns:
            Two items:
                * the retrieved object string ID set/frozenset, or None,
                  if no specification was provided with the pattern;
                * the provided list of object string ID sets/frozensets,
                  possibly with the first element removed, if it was consumed
                  by a placeholder.

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
            return None, obj_str_id_set_list
        if string == "%":
            if obj_str_id_set_list is None:
                raise Exception(
                    "No ID set list specified to substitute the placeholder"
                )
            try:
                return obj_str_id_set_list[0], obj_str_id_set_list[1:]
            except IndexError:
                raise Exception("Not enough ID lists specified") from None
        # Parse the ID list inside brackets, if any
        brackets_contents = string[1:-1].strip()
        return (Pattern._parse_id_list(brackets_contents)
                if brackets_contents else frozenset()), obj_str_id_set_list

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
    def parse(string, obj_str_id_set_list=None, schema=None):
        """
        Parse a pattern string and its parameter string IDs into a tree of
        Pattern objects. See kcidb.orm.query.Pattern.STRING_DOC for
        documentation on pattern strings.

        Args:
            string:                 The pattern string.
            obj_str_id_set_list:    A list of sets/frozensets of string IDs to
                                    use to filter the referenced objects with,
                                    in the order corresponding to the pattern
                                    string. Each string ID is a tuple with ID
                                    column value strings. If not specified, or
                                    specified as None, ID list placeholders
                                    are not allowed in the pattern string.
            schema:                 An object type schema to use, or None to
                                    use kcidb.orm.data.SCHEMA.

        Returns:
            A set of leaf Pattern objects selecting the ORM objects matched by
            the pattern string.
        """
        assert isinstance(string, str)
        assert obj_str_id_set_list is None or (
            isinstance(obj_str_id_set_list, list) and
            all(
                isinstance(obj_str_id_set, (set, frozenset)) and
                all(isinstance(obj_str_id, tuple) and
                    all(isinstance(str_part, (str, type(None)))
                        for str_part in obj_str_id)
                    for obj_str_id in obj_str_id_set)
                for obj_str_id_set in obj_str_id_set_list
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
            obj_str_id_set, obj_str_id_set_list = Pattern._parse_spec(
                spec, obj_str_id_set_list
            )
            try:
                base_set = Pattern._expand(
                    schema, base_set, match_set, relation == ">",
                    obj_type_expr, obj_str_id_set, match_spec
                )
            except Exception as exc:
                raise Exception(
                    f"Failed expanding pattern specification "
                    f"at position {pos}: {string[pos:]!r}"
                ) from exc
            pos = match.end()
        if obj_str_id_set_list:
            raise Exception(
                f"Too many ID sets specified for pattern {string!r}"
            )
        return match_set

    @staticmethod
    def from_io(io_data, schema=None, max_objs=0, copy=True):
        """
        Create a pattern set matching all objects in the supplied I/O data.

        Args:
            io_data:    The I/O data to create the pattern list from.
                        Must adhere to the current, or earlier schema version.
            schema:     An object type schema to use, or None to use
                        kcidb.orm.data.SCHEMA.
            max_objs:   Maximum number of object IDs to put into each
                        created pattern (a positive integer).
                        Zero for no limit.
            copy:       True if the io_data should be copied before the
                        (possibly-necessary) upgrade. False if it should be
                        upgraded in-place (if necessary).

        Returns:
            A set of Pattern objects matching the objects in the supplied I/O
            data.
        """
        io_schema = io.SCHEMA.get_exactly_compatible(io_data)
        assert io_schema is not None
        assert LIGHT_ASSERTS or io.SCHEMA.is_valid(io_data)
        assert schema is None or isinstance(schema, Schema)
        assert isinstance(max_objs, int) and max_objs >= 0

        if schema is None:
            schema = SCHEMA

        # The I/O schema version which breaks backwards compatibility
        breaking_io_schema = io.schema.V5_0
        # Do not upgrade across breaking boundary
        if io_schema < breaking_io_schema:
            io_schema = breaking_io_schema.previous
        else:
            io_schema = io.SCHEMA
        io_data = io_schema.upgrade(io_data, copy=copy)

        # Assert all I/O object lists are represented in the OO schema
        assert set(schema.types) >= \
            set(k[:-1] for k in io_schema.graph if k), \
            "Specified OO types are not a superset of I/O types"
        # Generate a pattern set matching all objects
        pattern_set = set()
        for obj_list_name in io_schema.graph:
            if not obj_list_name:
                continue
            assert obj_list_name.endswith("s")
            obj_list = io_data.get(obj_list_name, [])
            if not obj_list:
                continue
            obj_type = schema.types[obj_list_name[:-1]]
            id_field_types = obj_type.id_field_types
            for obj_list_slice in kcidb.misc.isliced(obj_list, max_objs):
                pattern_set.add(
                    Pattern(None, True, obj_type, {
                        tuple(o[id_field] for id_field in id_field_types)
                        for o in obj_list_slice
                    })
                )
        return pattern_set


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
