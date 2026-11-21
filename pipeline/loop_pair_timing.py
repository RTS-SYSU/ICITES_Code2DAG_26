"""Loop L2 directory adapter; reuse L1 strict validation and unchanged copy formula."""
import argparse
import hashlib
import json
from pathlib import Path
from .io_utils import read_json,write_json
from .loop_timing import _read_loop_timing_json,_validate_loop_segments
from .loop_model import copy_loop_timing,LoopModelError


def import_loop_pair_timing(*,base_dir,base_name,input_timing):
    root=Path(base_dir)/'intermediate_results'/base_name/'pipeline'
    segments=read_json(root/'blocks/level2/loop_static_pair/segments.json');source=Path(segments['source_file'])
    if hashlib.sha256(source.read_bytes()).hexdigest()!=segments['source_sha256']:raise LoopModelError('source changed since loop blocks generation')
    _validate_loop_segments(segments);result=copy_loop_timing(segments,_read_loop_timing_json(input_timing))
    result['input_timing_sha256']=hashlib.sha256(Path(input_timing).read_bytes()).hexdigest()
    write_json(root/'timing/level2/loop_static_pair/timing.json',result)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--base-dir',type=Path,required=True);p.add_argument('--base-name',required=True);p.add_argument('--input-timing',type=Path,required=True)
    print(json.dumps(import_loop_pair_timing(**vars(p.parse_args())),indent=2))


if __name__=='__main__':main()
