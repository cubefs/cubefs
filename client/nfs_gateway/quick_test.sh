#!/bin/bash
set -e

# Get project root directory (two levels up from script location)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

BIN="/tmp/nfs-gateway/nfs-gateway"
CONFIG="/tmp/nfs-gateway/client.config"

echo "=== Quick Test: Build, Run, Check Logs ==="
echo "Project root: $PROJECT_ROOT"
echo ""

# Prepare directories
echo "[1] Preparing directories..."
mkdir -p /tmp/nfs-gateway
if [ ! -d "/tmp/nfs-gateway" ]; then
    echo "❌ Failed to create /tmp/nfs-gateway directory"
    exit 1
fi

# Build
echo "[2] Building..."
cd "$PROJECT_ROOT"
BUILD_OUTPUT=$(bash build/build.sh nfs_gateway 2>&1)
BUILD_EXIT=$?
if echo "$BUILD_OUTPUT" | tail -1 | grep -q "success"; then
    echo "✅ Build OK"
    # Copy binary to /tmp/nfs-gateway
    if [ -f "build/bin/nfs-gateway" ]; then
        cp build/bin/nfs-gateway "$BIN"
        chmod +x "$BIN"
        echo "✅ Binary copied to $BIN"
    else
        echo "❌ Binary not found at build/bin/nfs-gateway"
        exit 1
    fi
else
    echo "❌ Build failed"
    echo ""
    echo "Build output:"
    echo "$BUILD_OUTPUT"
    exit 1
fi

# Cleanup
echo "[3] Cleaning..."
pkill -9 nfs-gateway 2>/dev/null || true
sudo umount -l "/tmp/nfs_test_mount" 2>/dev/null || true
sleep 1

# Create config file
echo "[4] Creating config file..."
cat > "$CONFIG" << 'EOF'
{
  "mountPoint": "/tmp/nfs-gateway/mount",
  "volName": "ltptest",
  "owner": "ltptest",
  "exporterPort": "17510",
  "masterAddr": "192.168.0.11:17010",
  "logDir": "/tmp/nfs-gateway/log",
  "logLevel": "debug",
  "rdonly": false,
  "enableXattr": true,
  "enablePosixACL": true
}
EOF
echo "✅ Config file created at: $CONFIG"
LOG_DIR=$(grep '"logDir"' "$CONFIG" | sed 's/.*"logDir"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/' | head -1)
# Default log directory if not specified in config
if [ -z "$LOG_DIR" ]; then
    LOG_DIR="/tmp/nfs-gateway/logs"
fi
echo "Config: $CONFIG"
echo "LogDir: $LOG_DIR"
mkdir -p "$LOG_DIR" 2>/dev/null || true

# Start server
echo "[5] Starting server..."
"$BIN" -config "$CONFIG" -port 20490 > "/tmp/nfs-gateway/server.log" 2>&1 &
PID=$!
sleep 60

if ! ps -p $PID > /dev/null 2>&1; then
    echo "❌ Server failed"
    cat "/tmp/nfs-gateway/server.log"
    exit 1
fi
echo "✅ Server running (PID: $PID)"

# Check logs
echo "[6] Server log:"
tail -20 "/tmp/nfs-gateway/server.log"
echo ""

# Check SDK logs
echo "[7] SDK logs:"
if [ -n "$LOG_DIR" ] && [ -d "$LOG_DIR" ]; then
    SDK_LOGS=$(find "$LOG_DIR" -name "nfs_gateway*.log" -type f -mmin -5 2>/dev/null | head -3)
    if [ -n "$SDK_LOGS" ]; then
        echo "✅ Found SDK logs:"
        ls -lh $SDK_LOGS | head -3
        echo ""
        echo "Latest log (last 20 lines):"
        find "$LOG_DIR" -name "nfs_gateway*.log" -type f -mmin -5 -exec tail -20 {} \; 2>/dev/null | head -25
    else
        echo "⚠️  No SDK logs found (may need more time or logDir issue)"
    fi
else
    echo "⚠️  LogDir not configured or missing"
fi

# Test mount
echo ""
echo "[8] Testing mount..."
sudo mkdir -p "/tmp/nfs_test_mount"
if sudo mount -t nfs -o vers=3,port=20490,mountport=20490,proto=tcp,nfsvers=3 localhost:/ "/tmp/nfs_test_mount" 2>&1; then
    echo "✅ Mount OK"
    ls "/tmp/nfs_test_mount" 2>&1 | head -3
    
    # Clean up test file if exists
    TEST_FILE="/tmp/nfs_test_mount/test.txt"
    sudo rm -f "$TEST_FILE" 2>/dev/null || true
    
    # Test 1: Read non-existent file (should fail)
    echo ""
    echo "Test 1: Reading non-existent file (should fail)..."
    if sudo cat "$TEST_FILE" 2>&1; then
        echo "❌ ERROR: Reading non-existent file should fail but succeeded!"
    else
        echo "✅ Correctly failed to read non-existent file"
    fi
    
    # Test 2: Write file
    echo ""
    echo "Test 2: Writing file..."
    TEST_CONTENT="test_$(date +%s)"
    if echo "$TEST_CONTENT" | sudo tee "$TEST_FILE" >/dev/null 2>&1; then
        echo "✅ Write OK"
    else
        echo "❌ Write failed"
    fi

    sleep 1
    
    # Test 3: Read existing file (should succeed)
    echo ""
    echo "Test 3: Reading existing file (should succeed)..."
    if READ_CONTENT=$(sudo cat "$TEST_FILE" 2>&1); then
        if [ "$READ_CONTENT" = "$TEST_CONTENT" ]; then
            echo "✅ Read OK (content matches)"
        else
            echo "⚠️  Read OK but content mismatch: expected='$TEST_CONTENT', got='$READ_CONTENT'"
        fi
    else
        echo "❌ Read failed"
    fi
    
    # Test 4: Create directory
    echo ""
    echo "Test 4: Creating directory..."
    TEST_DIR="/tmp/nfs_test_mount/test_dir_$(date +%s)"
    if sudo mkdir -p "$TEST_DIR" 2>&1; then
        echo "✅ Create directory OK: $TEST_DIR"
    else
        echo "❌ Create directory failed"
    fi
    
    # Test 5: Create file in directory
    echo ""
    echo "Test 5: Creating file in directory..."
    TEST_FILE_IN_DIR="$TEST_DIR/file_in_dir.txt"
    TEST_CONTENT_IN_DIR="content_in_dir_$(date +%s)"
    if echo "$TEST_CONTENT_IN_DIR" | sudo tee "$TEST_FILE_IN_DIR" >/dev/null 2>&1; then
        echo "✅ Create file in directory OK"
    else
        echo "❌ Create file in directory failed"
    fi
    
    # Test 6: Rename file
    echo ""
    echo "Test 6: Renaming file..."
    RENAMED_FILE="/tmp/nfs_test_mount/test_renamed_$(date +%s).txt"
    if sudo mv "$TEST_FILE" "$RENAMED_FILE" 2>&1; then
        if [ -f "$RENAMED_FILE" ]; then
            echo "✅ Rename file OK: $TEST_FILE -> $RENAMED_FILE"
        else
            echo "❌ Rename file failed: target file not found"
        fi
    else
        echo "❌ Rename file failed"
    fi
    
    # Test 7: Rename directory
    echo ""
    echo "Test 7: Renaming directory..."
    RENAMED_DIR="/tmp/nfs_test_mount/test_dir_renamed_$(date +%s)"
    if sudo mv "$TEST_DIR" "$RENAMED_DIR" 2>&1; then
        if [ -d "$RENAMED_DIR" ]; then
            echo "✅ Rename directory OK: $TEST_DIR -> $RENAMED_DIR"
            # Update file path
            TEST_FILE_IN_DIR="$RENAMED_DIR/file_in_dir.txt"
        else
            echo "❌ Rename directory failed: target directory not found"
        fi
    else
        echo "❌ Rename directory failed"
    fi
    
    # Test 8: Delete file
    echo ""
    echo "Test 8: Deleting file..."
    if sudo rm -f "$RENAMED_FILE" 2>&1; then
        if [ ! -f "$RENAMED_FILE" ]; then
            echo "✅ Delete file OK: $RENAMED_FILE"
        else
            echo "❌ Delete file failed: file still exists"
        fi
    else
        echo "❌ Delete file failed"
    fi
    
    # Test 9: Delete file in directory
    echo ""
    echo "Test 9: Deleting file in directory..."
    if sudo rm -f "$TEST_FILE_IN_DIR" 2>&1; then
        if [ ! -f "$TEST_FILE_IN_DIR" ]; then
            echo "✅ Delete file in directory OK: $TEST_FILE_IN_DIR"
        else
            echo "❌ Delete file in directory failed: file still exists"
        fi
    else
        echo "❌ Delete file in directory failed"
    fi
    
    # Test 10: Delete directory
    echo ""
    echo "Test 10: Deleting directory..."
    if sudo rmdir "$RENAMED_DIR" 2>&1; then
        if [ ! -d "$RENAMED_DIR" ]; then
            echo "✅ Delete directory OK: $RENAMED_DIR"
        else
            echo "❌ Delete directory failed: directory still exists"
        fi
    else
        echo "❌ Delete directory failed (may not be empty or have issues)"
    fi
    
    sudo umount "/tmp/nfs_test_mount" 2>/dev/null || true
else
    echo "❌ Mount failed"
fi

# Final logs
echo ""
echo "[9] Final server log:"
tail -15 "/tmp/nfs-gateway/server.log"

# Cleanup
kill $PID 2>/dev/null || true
echo ""
echo "Done"
