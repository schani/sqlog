#!/usr/bin/env bash
# Download RustFS 1.0.6 into ~/tools/rustfs, start it on 127.0.0.1:9000 and create the bucket.
# Usage: rustfs.sh [bucket]   (log: ~/tools/rustfs-logs/rustfs.log)
set -euo pipefail
dir=~/tools/rustfs logs=~/tools/rustfs-logs bucket=${1:-sqlog}
mkdir -p "$dir/data" "$logs"
if [ ! -x "$dir/rustfs" ]; then
  (cd "$dir" && curl -fsSLO https://github.com/rustfs/rustfs/releases/download/1.0.6/rustfs-linux-x86_64-musl-v1.0.6.zip \
    && unzip -oq rustfs-linux-x86_64-musl-v1.0.6.zip)
fi
RUSTFS_CONSOLE_ENABLE=false nohup "$dir/rustfs" server --address 127.0.0.1:9000 \
  --access-key testak --secret-key testsk "$dir/data" >"$logs/rustfs.log" 2>&1 &
echo "rustfs pid $!"
sleep 2
# RustFS requires the payload hash header, which curl's SigV4 doesn't send by itself.
curl -fsS --aws-sigv4 "aws:amz:us-east-1:s3" --user testak:testsk -X PUT \
  -H "x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" \
  "http://127.0.0.1:9000/$bucket" && echo "bucket $bucket created" || echo "bucket $bucket not created (exists?)"
