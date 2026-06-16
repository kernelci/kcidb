"""
Kernel CI BigQuery report database - miscellaneous schema definitions
"""
import re
from google.cloud.bigquery.schema import SchemaField as Field
from google.protobuf import \
    descriptor_pb2, descriptor_pool, message_factory


class JSONInvalidError(Exception):
    """JSON doesn't match table schema error"""


def validate_json_value(field, value):
    """
    Validate a JSON value against the schema for the field it's supposed to be
    loaded into.

    Args:
        field:  The field schema to validate against, an instance of
                google.cloud.bigquery.schema.SchemaField.
        value:  The JSON value to validate.

    Returns:
        The validated JSON value.

    Raises:
        JSONInvalidError: the JSON value doesn't match the field type.
    """
    # It's OK, pylint: disable=too-many-branches
    assert isinstance(field, Field)
    if field.field_type == "BOOL":
        if not isinstance(value, bool):
            raise JSONInvalidError(f"Value is not a boolean: {value!r}")
    elif field.field_type == "STRING":
        if not isinstance(value, str):
            raise JSONInvalidError(f"Value is not a string: {value!r}")
    elif field.field_type == "INTEGER":
        if not isinstance(value, int):
            raise JSONInvalidError(f"Value is not integer: {value!r}")
    elif field.field_type == "FLOAT64":
        if not isinstance(value, (int, float)):
            raise JSONInvalidError(f"Value is not numeric: {value!r}")
    elif field.field_type == "TIMESTAMP":
        if not isinstance(value, str):
            raise JSONInvalidError(
                f"Timestamp value is not a string: {value!r}"
            )
    elif field.field_type == "RECORD":
        if not isinstance(value, dict):
            raise JSONInvalidError(
                f"Record value is not a dictionary: {value!r}"
            )
        validate_json_obj(field.fields, value)
    else:
        raise ValueError(f"Unknown field type: {field.field_type!r}")

    return value


def validate_json_obj(field_list, obj):
    """
    Validate a JSON object against the schema of the table it's to be loaded
    into.

    Args:
        field_list: The list of table fields to validate against. A list of
                    google.cloud.bigquery.schema.SchemaField instances.
        obj:        The JSON object to validate.

    Returns:
        The validated JSON object.

    Raises:
        JSONInvalidError: the JSON object doesn't match the schema.
    """
    assert isinstance(field_list, (tuple, list)) and \
           all(isinstance(f, Field) for f in field_list)
    assert isinstance(obj, dict)

    field_map = {f.name: f for f in field_list}
    unseen_field_map = field_map.copy()
    for name, value in obj.items():
        try:
            field = field_map[name]
            unseen_field_map.pop(name, None)
        except KeyError:
            raise JSONInvalidError(f"Unknown field: {name!r}") from None
        try:
            if field.mode == "REPEATED":
                if not isinstance(value, list):
                    raise JSONInvalidError(
                        f"Repeated field value is not a list: {value!r}"
                    )
                for idx, item in enumerate(value):
                    try:
                        validate_json_value(field, item)
                    except JSONInvalidError as err:
                        raise JSONInvalidError(
                            f"Repeated value at index {idx} is invalid"
                        ) from err
            else:
                validate_json_value(field, value)
        except JSONInvalidError as err:
            raise JSONInvalidError(
                f"Value of field {name!r} is invalid"
            ) from err

    for unseen_name, unseen_field in unseen_field_map.items():
        if unseen_field.mode == "REQUIRED":
            raise JSONInvalidError(
                f"Required field {unseen_name!r} is missing"
            )

    return obj


def validate_json_obj_list(field_list, obj_list):
    """
    Validate a list of JSON objects against the schema of the table they are
    to be loaded into.

    Args:
        field_list: The list of table fields to validate against. A list of
                    google.cloud.bigquery.schema.SchemaField instances.
        obj_list:   The list of JSON objects to validate.

    Returns:
        The validated JSON object list.

    Raises:
        JSONInvalidError: the JSON object doesn't match the schema.
    """
    assert isinstance(field_list, list) and \
           all(isinstance(f, Field) for f in field_list)
    assert isinstance(obj_list, list) and \
           all(isinstance(o, dict) for o in obj_list)

    for obj in obj_list:
        validate_json_obj(field_list, obj)

    return obj_list

# You're wrong, pylint: disable=no-member


# The Protobuf field types corresponding to BigQuery field types
PROTOBUF_FIELD_TYPES = {
    names[1]: getattr(descriptor_pb2.FieldDescriptorProto, "TYPE_" + names[2])
    for names in re.finditer(
        r"\s*(\S+)\s+(\S+)\s*\n",
        """
            STRING      STRING
            BYTES       BYTES
            INTEGER     INT64
            INT64       INT64
            FLOAT       DOUBLE
            FLOAT64     DOUBLE
            BOOLEAN     BOOL
            BOOL        BOOL
            TIMESTAMP   STRING
            DATE        STRING
            TIME        STRING
            DATETIME    STRING
            NUMERIC     STRING
            BIGNUMERIC  STRING
            GEOGRAPHY   STRING
        """
    )
}


def protobuf_msg_desc_fill(desc, field_list):
    """
    Fill a protobuf message descriptor with definitions from a (BigQuery
    schema) field list.

    Args:
        desc:       The protobuf message descriptor to fill with the field
                    list definition.
        field_list: The list/tuple of BigQuery SchemaField objects to describe
                    as a message.

    Returns:
        The filled protobuf message descriptor.
    """
    assert isinstance(desc, descriptor_pb2.DescriptorProto)
    assert isinstance(field_list, (list, tuple))
    assert all(isinstance(field, Field) for field in field_list)

    # It's a type, pylint: disable=invalid-name
    FDP = descriptor_pb2.FieldDescriptorProto

    for i, field in enumerate(field_list, start=1):
        field_desc = desc.field.add()
        field_desc.name = field.name
        field_desc.number = i

        if field.mode == "REPEATED":
            field_desc.label = FDP.LABEL_REPEATED
        else:
            field_desc.label = FDP.LABEL_OPTIONAL

        if field.field_type.upper() == "RECORD":
            protobuf_msg_desc_fill(
                desc.nested_type.add(), field.fields
            ).name = field.name
            field_desc.type = FDP.TYPE_MESSAGE
            field_desc.type_name = field.name
        else:
            field_desc.type = PROTOBUF_FIELD_TYPES[field.field_type]
            if field.mode != "REPEATED":
                field_desc.proto3_optional = True
                oneof = desc.oneof_decl.add()
                oneof.name = "_" + field.name
                field_desc.oneof_index = len(desc.oneof_decl) - 1

    return desc


def protobuf_file_desc_create(name, field_list):
    """
    Create a protobuf file descriptor from a BigQuery table schema field list.

    Args:
        name:       The name of the object type stored in the table
                    (singular). Will be used for naming the file
                    (as <name>.proto) and the message type (literally).
                    The file will belong to "org.kernelci.bq" package.
        field_list: The list of SchemaField objects defining the table schema.

    Returns:
        The created file descriptor.
    """
    assert isinstance(name, str)
    assert name.isidentifier()
    assert isinstance(field_list, (list, tuple))
    assert all(isinstance(field, Field) for field in field_list)

    desc = descriptor_pb2.FileDescriptorProto(
        package="org.kernelci.bq",
        name=name + ".proto",
    )
    protobuf_msg_desc_fill(desc.message_type.add(), field_list).name = name

    return desc


def protobuf_msg_type_create(name, field_list):
    """
    Create a protobuf message class for a table with the schema described by a
    list of SchemaField objects.

    Args:
        name:       The name of the object type stored in the table
                    (singular). Will be used for naming the message type.
        field_list: The list of SchemaField objects defining the table schema.

    Returns:
        The created message class.
    """
    assert isinstance(name, str)
    assert name.isidentifier()
    assert isinstance(field_list, (list, tuple))
    assert all(isinstance(field, Field) for field in field_list)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(protobuf_file_desc_create(name, field_list))
    return message_factory.GetMessageClass(
        pool.FindMessageTypeByName("org.kernelci.bq." + name)
    )
