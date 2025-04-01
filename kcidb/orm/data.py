"""
The data structures and constants used by the KCIDB object-relational
mapping system to organize Kernel CI report data into objects.
"""

import jsonschema
from kcidb.misc import LIGHT_ASSERTS
import kcidb.io as io


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
        assert len(ref_fields) == len(parent.id_field_types)
        self.parent = parent
        self.child = child
        self.ref_fields = ref_fields


class Type:
    """An object type"""

    # It's OK, pylint: disable=too-many-instance-attributes,too-many-arguments

    def __init__(self, name, json_schema, id_field_types):
        """
        Initialize an object type.

        Args:
            name:                   The type name.
            json_schema:            The JSON schema describing the raw object
                                    data.
            id_field_types:         A dictionary containing the names of object
                                    fields identifying it globally, and their
                                    Python types.
        """
        assert isinstance(name, str)
        assert isinstance(json_schema, dict)
        assert isinstance(id_field_types, dict) and \
               all(isinstance(f, str) and isinstance(t, type)
                   for f, t in id_field_types.items())

        # The name of this type
        self.name = name
        # The JSON schema for this type's raw data
        self.json_schema = json_schema
        # Map of names of ID fields to their types
        self.id_field_types = id_field_types
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
        format_checker = jsonschema.Draft7Validator.FORMAT_CHECKER
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
        return tuple(data[field] for field in self.id_field_types)

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
                        * "id_field_types" - a dictionary containing the names
                                             of object fields identifying it
                                             globally, and their types,
                        * "children" - the optional dictionary of names of
                                       child types and tuples containing
                                       names of fields with values of parent's
                                       identifying fields ("id_field_types"
                                       keys), in the same order.
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
            "id_field_types" in info and
            isinstance(info["id_field_types"], dict) and
            (set(info["id_field_types"]) <=
             set(info["field_json_schemas"])) and
            all(isinstance(f, str) and isinstance(t, type)
                for f, t in info["id_field_types"].items()) and
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
            self.types[name] = Type(name, json_schema, info["id_field_types"])

        # Create and register relations
        self.relations = []
        for name, info in types.items():
            obj_type = self.types[name]
            for child_name, ref_fields in info.get("children", {}).items():
                try:
                    child_type = self.types[child_name]
                except KeyError:
                    raise Exception(f"Couldn't find child {child_name!r} "
                                    f"of type {name!r}") from None
                relation = Relation(obj_type, child_type, ref_fields)
                self.relations.append(relation)
                obj_type.add_relation(relation)
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
        format_checker = jsonschema.Draft7Validator.FORMAT_CHECKER
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
# Test numerical test output properties from the current I/O schema
_TEST_NUMBER = _TEST['number']['properties']

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
            ),
            required_fields=set(),
            id_field_types=dict(git_commit_hash=str, patchset_hash=str),
            children=dict(
                checkout=("git_commit_hash", "patchset_hash",)
            ),
        ),
        checkout=dict(
            field_json_schemas=dict(
                id=_CHECKOUT['id'],
                git_commit_hash=_CHECKOUT['git_commit_hash'],
                git_commit_tags=_CHECKOUT['git_commit_tags'],
                git_commit_message=_CHECKOUT['git_commit_message'],
                patchset_hash=_CHECKOUT['patchset_hash'],
                origin=_CHECKOUT['origin'],
                git_repository_url=_CHECKOUT['git_repository_url'],
                git_repository_branch=_CHECKOUT['git_repository_branch'],
                git_repository_branch_tip=_CHECKOUT[
                    'git_repository_branch_tip'
                ],
                tree_name=_CHECKOUT['tree_name'],
                message_id=_CHECKOUT['message_id'],
                start_time=_CHECKOUT['start_time'],
                log_url=_CHECKOUT['log_url'],
                comment=_CHECKOUT['comment'],
                valid=_CHECKOUT['valid'],
                misc=_CHECKOUT['misc'],
            ),
            required_fields={'id', 'origin'},
            id_field_types=dict(id=str),
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
                comment=_BUILD['comment'],
                status=_BUILD['status'],
                misc=_BUILD['misc'],
            ),
            required_fields={'id', 'origin', 'checkout_id'},
            id_field_types=dict(id=str),
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
                environment_compatible=_TEST_ENVIRONMENT['compatible'],
                environment_misc=_TEST_ENVIRONMENT['misc'],
                status=_TEST['status'],
                number_value=_TEST_NUMBER['value'],
                number_unit=_TEST_NUMBER['unit'],
                number_prefix=_TEST_NUMBER['prefix'],
                start_time=_TEST['start_time'],
                duration=_TEST['duration'],
                output_files=_TEST['output_files'],
                log_url=_TEST['log_url'],
                comment=_TEST['comment'],
                misc=_TEST['misc'],
            ),
            required_fields={'id', 'origin', 'build_id'},
            id_field_types=dict(id=str),
            children=dict(
                incident=("test_id",),
            ),
        ),
        issue=dict(
            field_json_schemas=dict(
                id=_ISSUE['id'],
                origin=_ISSUE['origin'],
            ),
            required_fields={'id', 'origin'},
            id_field_types=dict(id=str),
            children=dict(
                issue_version=("id",),
            ),
        ),
        issue_version=dict(
            field_json_schemas=dict(
                id=_ISSUE['id'],
                version_num=_ISSUE['version'],
                origin=_ISSUE['origin'],
                report_url=_ISSUE['report_url'],
                report_subject=_ISSUE['report_subject'],
                culprit_code=_ISSUE_CULPRIT['code'],
                culprit_tool=_ISSUE_CULPRIT['tool'],
                culprit_harness=_ISSUE_CULPRIT['harness'],
                comment=_ISSUE['comment'],
                misc=_ISSUE['misc'],
            ),
            required_fields={'id', 'version_num', 'origin'},
            id_field_types=dict(id=str, version_num=int),
            children=dict(
                incident=("issue_id", "issue_version_num"),
            ),
        ),
        incident=dict(
            field_json_schemas=dict(
                id=_INCIDENT['id'],
                origin=_INCIDENT['origin'],
                issue_id=_INCIDENT['issue_id'],
                issue_version_num=_INCIDENT['issue_version'],
                build_id=_INCIDENT['build_id'],
                test_id=_INCIDENT['test_id'],
                present=_INCIDENT['present'],
                comment=_INCIDENT['comment'],
                misc=_INCIDENT['misc'],
            ),
            required_fields={'id', 'origin', 'issue_id', 'issue_version_num'},
            id_field_types=dict(id=str),
        ),
    )
)

assert all(k.endswith("s") for k in io.SCHEMA.graph if k), \
    "Not all I/O object list names end with 's'"

assert set(SCHEMA.types) >= \
    set(k[:-1] for k in io.SCHEMA.graph if k), \
    "OO types are not a superset of I/O types"
