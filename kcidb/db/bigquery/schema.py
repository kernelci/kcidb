"""
Kernel CI BigQuery report database - miscellaneous schema definitions
"""
from google.cloud.bigquery.schema import SchemaField as Field


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
