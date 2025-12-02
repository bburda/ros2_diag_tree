#!/usr/bin/env python3
# Copyright 2025 mfaferek93
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Generate JSON schema for ROS 2 message types.

This script uses rosidl_runtime_py to introspect message types and generate
a JSON schema representation that includes field names and types recursively.

Usage:
    python3 get_type_schema.py <message_type>
    python3 get_type_schema.py sensor_msgs/msg/Temperature

Output:
    JSON object with "name" and "schema" fields, or "error" on failure.
"""

import json
import sys

from rosidl_parser.definition import (
    Array,
    BasicType,
    BoundedSequence,
    BoundedString,
    NamespacedType,
    UnboundedSequence,
    UnboundedString,
)
from rosidl_runtime_py.utilities import get_message


def type_to_dict(slot_type):
    """
    Convert a rosidl type definition to a JSON-serializable dictionary.

    Args
    ----
        slot_type: A rosidl_parser.definition type object

    Returns
    -------
        dict: JSON-serializable representation of the type

    """
    if isinstance(slot_type, BasicType):
        return {'type': slot_type.typename}

    elif isinstance(slot_type, (UnboundedString, BoundedString)):
        result = {'type': 'string'}
        if isinstance(slot_type, BoundedString):
            result['max_length'] = slot_type.maximum_size
        return result

    elif isinstance(slot_type, NamespacedType):
        # Nested message type - recursively get its schema
        nested_name = '/'.join(slot_type.namespaces + [slot_type.name])
        try:
            nested_class = get_message(nested_name)
            return {
                'type': nested_name,
                'fields': get_message_schema(nested_class)
            }
        except (ModuleNotFoundError, AttributeError, ImportError):
            # If we can't load the nested type, just return the name
            return {'type': nested_name}

    elif isinstance(slot_type, (Array, UnboundedSequence, BoundedSequence)):
        result = {
            'type': 'array',
            'items': type_to_dict(slot_type.value_type)
        }
        if isinstance(slot_type, Array):
            result['size'] = slot_type.size
        elif isinstance(slot_type, BoundedSequence):
            result['max_size'] = slot_type.maximum_size
        return result

    else:
        # Fallback for unknown types
        return {'type': str(type(slot_type).__name__)}


def get_message_schema(msg_class):
    """
    Get the full schema for a message class.

    Args
    ----
        msg_class: A ROS 2 message class

    Returns
    -------
        dict: Schema with field names as keys and type info as values

    """
    schema = {}
    for slot, slot_type in zip(msg_class.__slots__, msg_class.SLOT_TYPES):
        # Skip internal fields (start with underscore but aren't real fields)
        if slot.startswith('_') and slot != '_check_fields':
            field_name = slot[1:]  # Remove leading underscore
            schema[field_name] = type_to_dict(slot_type)
    return schema


def main():
    """Execute schema generation from command line."""
    if len(sys.argv) != 2:
        result = {
            'error': 'Usage: get_type_schema.py <message_type>',
            'example': 'get_type_schema.py sensor_msgs/msg/Temperature'
        }
        print(json.dumps(result))
        sys.exit(1)

    type_name = sys.argv[1]

    try:
        msg_class = get_message(type_name)
        schema = get_message_schema(msg_class)
        result = {
            'name': type_name,
            'schema': schema
        }
        print(json.dumps(result))
    except Exception as e:  # noqa: BLE001 - Catch all for JSON error reporting
        result = {
            'error': f"Failed to get schema for '{type_name}': {str(e)}",
            'type': type_name
        }
        print(json.dumps(result))
        sys.exit(1)


if __name__ == '__main__':
    main()
