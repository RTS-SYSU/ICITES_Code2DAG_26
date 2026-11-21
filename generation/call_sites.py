"""Lossless RTL call occurrence ledger, separate from legacy name-keyed maps."""
import hashlib
import re
from pathlib import Path


def collect_call_sites(rtl_files):
    result = []
    for path in rtl_files:
        path = Path(path)
        data=path.read_bytes()
        digest=hashlib.sha256(data).hexdigest()
        text=data.decode(errors='replace')
        caller = None
        pending = None
        for line in text.splitlines():
            header = re.match(r';; Function .*?\(([^,\s]+)', line)
            if header:
                if pending:result.append(pending);pending=None
                caller = header.group(1)
            call = re.search(r'\(call(?:\s|\[)', line)
            if call and caller:
                if pending:
                    result.append(pending)
                target = re.search(r'"([^\"]+)"', line)
                pending = dict(callsite_id='RTL:'+str(len(result)), caller=caller,
                               callee=target.group(1) if target else None,
                               rtl_file=str(path.resolve()), rtl_sha256=digest,
                               file=None, line=None, column=None)
            if pending:
                loc = re.search(r'"([^\"]+)":(\d+):(\d+)', line)
                if loc:
                    pending.update(file=loc.group(1), line=int(loc.group(2)), column=int(loc.group(3)))
                    result.append(pending)
                    pending = None
        if pending:
            result.append(pending)
    return result
