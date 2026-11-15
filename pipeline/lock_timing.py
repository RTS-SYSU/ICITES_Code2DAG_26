"""Import existing ungrouped timing; never compile, instrument, or run C."""
import argparse
import hashlib
import json
from pathlib import Path

from .io_utils import read_json, write_json
from .lock_model import aggregate_lock_timing, LockModelError


def import_lock_timing(*, base_dir, base_name, input_timing):
    root = Path(base_dir)/'intermediate_results'/base_name/'pipeline'
    segments = read_json(root/'blocks/level2/lock_grouped/segments.json')
    # Reject stale source while preserving the original file byte-for-byte.
    source = Path(segments['source_file'])
    if hashlib.sha256(source.read_bytes()).hexdigest()!=segments['source_sha256']:
        raise LockModelError('source changed since lock blocks generation')
    result = aggregate_lock_timing(segments,read_json(Path(input_timing)))
    result['input_timing_sha256'] = hashlib.sha256(Path(input_timing).read_bytes()).hexdigest()
    write_json(root/'timing/level2/lock_grouped/timing.json',result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-dir',type=Path,required=True)
    parser.add_argument('--base-name',required=True)
    parser.add_argument('--input-timing',type=Path,required=True)
    args = parser.parse_args()
    print(json.dumps(import_lock_timing(**vars(args)),indent=2))


if __name__=='__main__':
    main()
