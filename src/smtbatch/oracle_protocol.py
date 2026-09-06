"""Consumer validation for the versioned oracle envelope; no solver semantics here."""
import json

SCHEMA = 'd3smt-oracle-v3'
EXIT = {'interesting':0, 'not_interesting':1, 'incomplete':3, 'error':2}


def decode(stdout, returncode):
    try:
        value = json.loads(stdout)
    except (ValueError, TypeError, UnicodeError):
        return None
    if not isinstance(value,dict) or value.get('schema') != SCHEMA:
        return None
    if value.get('outcome') not in EXIT or returncode != EXIT[value['outcome']]:
        return None
    if not isinstance(value.get('reason'),str):
        return None
    return value


def preserving(left, right):
    return bool(left and right and left.get('outcome') == right.get('outcome') == 'interesting'
                and left.get('signature') is not None and left.get('signature') == right.get('signature'))
