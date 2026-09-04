import os
import re
import sys

def clean_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    original_content = content

    # 1. Remove import
    content = re.sub(r'^import "storage/datapol/annotations/proto/semantic_annotations\.proto";\n?', '', content, flags=re.MULTILINE)
    
    # 2. Remove option (datapol.file_vetting_status)
    content = re.sub(r'^option\s+\(datapol\.file_vetting_status\)\s*=\s*"[^"]+";\n?', '', content, flags=re.MULTILINE)
    
    # 3. Remove (datapol.qualifier) = { ... }, inside multi-line brackets
    content = re.sub(r'^\s*\(datapol\.qualifier\)\s*=\s*\{[^}]*\},\n?', '', content, flags=re.MULTILINE)
    
    # 4. Remove (datapol.semantic_type) = ..., inside multi-line brackets
    content = re.sub(r'^\s*\(datapol\.semantic_type\)\s*=\s*[A-Z_]+,\n?', '', content, flags=re.MULTILINE)
    
    # 5. Remove (datapol.semantic_type) = ... inside multi-line brackets but it's the last element (no trailing comma)
    content = re.sub(r'^\s*\(datapol\.semantic_type\)\s*=\s*[A-Z_]+\n?', '\n', content, flags=re.MULTILINE)
    
    # 6. Remove inside single-line brackets:
    # 6a. [(datapol.semantic_type) = ST_IDENTIFYING_ID]
    content = re.sub(r'\s*\[\s*\(datapol\.semantic_type\)\s*=\s*[A-Z_]+\s*\]', '', content)
    
    # 6b. [(datapol.semantic_type) = ST_..., other_option = ...] -> [other_option = ...]
    content = re.sub(r'\[\s*\(datapol\.semantic_type\)\s*=\s*[A-Z_]+,\s*', '[', content)
    
    # 6c. [other_option = ..., (datapol.semantic_type) = ST_...] -> [other_option = ...]
    content = re.sub(r',\s*\(datapol\.semantic_type\)\s*=\s*[A-Z_]+\s*\]', ']', content)
    
    # 6d. [other_option = ..., (datapol.semantic_type) = ST_..., another_option = ...] -> [other_option = ..., another_option = ...]
    content = re.sub(r',\s*\(datapol\.semantic_type\)\s*=\s*[A-Z_]+(?=\s*,)', '', content)

    if content != original_content:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"Cleaned {filepath}")

proto_dir = 'java-bigtable/proto-google-cloud-bigtable-v2/src/main/proto'
for root, _, files in os.walk(proto_dir):
    for file in files:
        if file.endswith('.proto'):
            clean_file(os.path.join(root, file))
