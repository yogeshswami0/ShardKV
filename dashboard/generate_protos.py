#!/usr/bin/env python3
"""
Generate Python protobuf files from ShardKV proto definitions.
"""

import subprocess
import os
import sys

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    proto_dir = os.path.join(os.path.dirname(script_dir), 'proto')
    output_dir = os.path.join(script_dir, 'proto_gen')
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Create __init__.py
    init_file = os.path.join(output_dir, '__init__.py')
    if not os.path.exists(init_file):
        open(init_file, 'w').close()
    
    # Proto files to compile
    protos = ['kv_service.proto', 'raft_service.proto']
    
    for proto in protos:
        proto_path = os.path.join(proto_dir, proto)
        if not os.path.exists(proto_path):
            print(f"Warning: {proto_path} not found")
            continue
        
        print(f"Generating Python code for {proto}...")
        
        cmd = [
            sys.executable, '-m', 'grpc_tools.protoc',
            f'-I{proto_dir}',
            f'--python_out={output_dir}',
            f'--grpc_python_out={output_dir}',
            proto_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return 1
        
        print(f"  ✓ Generated {proto.replace('.proto', '_pb2.py')}")
    
    print(f"\nProto files generated in {output_dir}")
    return 0

if __name__ == '__main__':
    sys.exit(main())
