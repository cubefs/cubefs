#!/bin/bash
# Script to download go-nfs source code to local directory for debugging

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NFS_DIR="${SCRIPT_DIR}/go-nfs-local"
VERSION="v0.0.3"
REPO="https://github.com/willscott/go-nfs.git"

echo "Setting up go-nfs source code for local debugging..."
echo "Target directory: ${NFS_DIR}"

# Check if directory already exists
if [ -d "${NFS_DIR}" ]; then
    echo "Directory ${NFS_DIR} already exists. Removing..."
    rm -rf "${NFS_DIR}"
fi

# Create directory
mkdir -p "${NFS_DIR}"

# Clone the repository
echo "Cloning go-nfs repository..."
cd "${NFS_DIR}"
git clone "${REPO}" .

# Checkout specific version
echo "Checking out version ${VERSION}..."
git checkout "${VERSION}" 2>/dev/null || git checkout "tags/${VERSION}" 2>/dev/null || echo "Warning: Could not checkout ${VERSION}, using default branch"

# Show structure
echo ""
echo "go-nfs source code structure:"
find . -type f -name "*.go" | head -20

echo ""
echo "Setup complete! Source code is in: ${NFS_DIR}"
echo ""
echo "Next steps:"
echo "1. Update go.mod to use replace directive:"
echo "   Add this line to go.mod (after other replace directives):"
echo "   replace github.com/willscott/go-nfs => ./go-nfs-local"
echo ""
echo "2. Run: go mod tidy"
echo ""
echo "3. Add logging to key files:"
echo "   - go-nfs-local/nfs_onwrite.go (WRITE operation)"
echo "   - go-nfs-local/nfs_onsetattr.go (SETATTR operation - likely issue source)"
echo "   - go-nfs-local/file.go (file operations)"
echo ""
echo "4. Rebuild: cd ../.. && bash build/build.sh nfs_demo_v2"
