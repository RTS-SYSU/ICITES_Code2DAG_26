"""Small shared source recognizers. Never rewrite the compiled C or its offsets.

Only global scalar enum typedefs and direct pthread calls with null arguments
are recognized here. Pointer/type aliases, wrapped calls and dynamic thread
creation remain the responsibility of (and rejected by) the existing models.
"""
from dataclasses import dataclass
import re
from typing import Optional

from ..core.func_ranges import _strip_comments_and_strings


NULL_ARGUMENT = r'(?:NULL|0)'
_IDENT = r'[A-Za-z_]\w*'
_INTEGER = r'[+-]?(?:0[xX][0-9a-fA-F]+|0[0-7]*|[1-9][0-9]*)'
_ENUM = re.compile(
    rf'\btypedef\s+enum(?:\s+{_IDENT})?\s*\{{([^{{}}]*)\}}\s*({_IDENT})\s*;'
)


def mask_supported_enum_typedefs(text):
    """Mask a conservative subset for lexical guards, keeping every offset.

    Unsupported typedefs remain visible to the caller's existing rejection.
    Comments/literals are sanitized for recognition; all other input characters
    are returned unchanged. Declarations inside a function/block are not masked.
    """
    clean = _strip_comments_and_strings(text)
    result = list(text)
    for match in _ENUM.finditer(clean):
        prefix = clean[:match.start()]
        if prefix.count('{') != prefix.count('}'):
            continue
        if match.group(2).startswith(('pthread_', 'sem_')):
            continue
        values = match.group(1).strip().rstrip(',').split(',')
        if not values or not all(re.fullmatch(
                rf'\s*{_IDENT}(?:\s*=\s*{_INTEGER})?\s*', value) for value in values):
            continue
        for index in range(match.start(), match.end()):
            if result[index] not in '\r\n':
                result[index] = ' '
    return ''.join(result)


class SourceSyntaxError(ValueError):
    pass


@dataclass(frozen=True)
class ThreadCall:
    handle: str
    entry: Optional[str]
    start: int
    end: int


def direct_thread_calls(text, operation, *, standalone=False):
    """Read every create/join or fail; do not silently drop unsupported sites.

    Accept NULL/0 equivalently. Entry functions must be bare names, attributes
    and user arguments must be null, and handles must be individual variables.
    The legacy source binder does not consistently support &entry, so neither
    does this recognizer. No function-pointer or argument-alias analysis occurs.
    """
    if operation not in ('create', 'join'):
        raise ValueError('expected create or join')
    clean = _strip_comments_and_strings(text)
    if operation == 'create':
        pattern = rf'pthread_create\s*\(\s*&\s*({_IDENT})\s*,\s*{NULL_ARGUMENT}\s*,\s*({_IDENT})\s*,\s*{NULL_ARGUMENT}\s*\)'
    else:
        pattern = rf'pthread_join\s*\(\s*({_IDENT})\s*,\s*{NULL_ARGUMENT}\s*\)'
    calls = []
    for site in re.finditer(rf'\bpthread_{operation}\b', clean):
        match = re.compile(pattern).match(clean, site.start())
        if match is None:
            raise SourceSyntaxError(f'unsupported direct pthread_{operation} syntax')
        if standalone:
            start = clean.rfind('\n', 0, match.start()) + 1
            end = clean.find('\n', match.end())
            if end < 0:
                end = len(clean)
            if clean[start:match.start()].strip() or clean[match.end():end].strip() != ';':
                raise SourceSyntaxError(f'pthread_{operation} must be a standalone statement')
        calls.append(ThreadCall(match.group(1), match.group(2) if operation == 'create' else None,
                                match.start(), match.end()))
    return calls
