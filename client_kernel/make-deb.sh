#!/bin/bash
# 构建 cubefs 内核客户端 deb 包(预编译 cubefs.ko + systemd 挂载单元)。
# 产物: <repo>/bin/client-kernel/cubefs-kmod_<ver>_<kver>_amd64.deb
# 由根 Makefile 的 `make client-kernel` 调用，也可单独运行。
#
# 关键: deb 内的 cubefs.ko 绑定当前内核 vermagic，只能装到相同内核版本的机器。
# 多内核版本需在各自内核环境分别构建。
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)          # client_kernel/
ROOT=$(cd "$HERE/.." && pwd)                 # cubefs 仓库根
SERVICES="$ROOT/services"
KVER=$(uname -r)
PKGVER=${PKGVER:-1.0}
OUT="$ROOT/bin/client-kernel"

command -v dpkg-deb >/dev/null || { echo "需要 dpkg-deb(Debian/Ubuntu)"; exit 1; }

# 1. 确保 cubefs.ko 已编译
if [ ! -f "$HERE/cubefs.ko" ]; then
  echo "==> cubefs.ko 不存在，先编译"
  ( cd "$HERE" && make )
fi
[ -f "$HERE/cubefs.ko" ] || { echo "cubefs.ko 编译失败"; exit 1; }

# 1b. 构建 metrics exporter(复用主 module 的 vendored client_golang)
echo "==> 构建 cubefs-client-exporter"
( cd "$ROOT" && GOOS=linux GOARCH=amd64 go build -mod=vendor -trimpath -ldflags='-s -w' \
    -o "$HERE/cubefs-client-exporter" ./cmd/cubefs-client-exporter )
[ -f "$HERE/cubefs-client-exporter" ] || { echo "exporter 编译失败"; exit 1; }

# 2. 组装包目录
P=$(mktemp -d)/cubefs-kmod
mkdir -p "$P/DEBIAN" \
         "$P/lib/modules/$KVER/extra" \
         "$P/lib/systemd/system" \
         "$P/usr/local/bin" \
         "$P/etc/cubefs"
cp "$HERE/cubefs.ko"                           "$P/lib/modules/$KVER/extra/"
cp "$SERVICES/cubefs-mount@.service"           "$P/lib/systemd/system/"
cp "$SERVICES/cubefs-client-exporter.service"  "$P/lib/systemd/system/"
cp "$HERE/cubefs-client-exporter"              "$P/usr/local/bin/"
cp "$SERVICES/cubefs.conf.example"             "$P/etc/cubefs/"
cp "$SERVICES/cubefs-exporter.conf.example"    "$P/etc/cubefs/"

# 3. control(包名+内核版本，多版本可共存仓库)
cat > "$P/DEBIAN/control" <<EOF
Package: cubefs-kmod
Version: ${PKGVER}-${KVER}
Architecture: amd64
Maintainer: cubefs <noreply@local>
Section: kernel
Priority: optional
Description: CubeFS kernel client module + systemd mount/metrics units
 Prebuilt cubefs.ko for kernel ${KVER}, systemd template unit
 cubefs-mount@.service for auto-mount, and cubefs-client-exporter
 (Prometheus metrics on :9970) for observability.
 Bound to this kernel version (vermagic) — install only on matching kernel.
EOF

# 4. conffiles(配置示例不被升级覆盖)
{
  echo "/etc/cubefs/cubefs.conf.example"
  echo "/etc/cubefs/cubefs-exporter.conf.example"
} > "$P/DEBIAN/conffiles"

# 5. postinst: 更新模块索引 + 重载 systemd
cat > "$P/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
depmod -a
[ -d /run/systemd/system ] && systemctl daemon-reload || true
echo "[cubefs-kmod] 已安装。配置自动挂载:"
echo "  cp /etc/cubefs/cubefs.conf.example /etc/cubefs/data.conf && vi /etc/cubefs/data.conf"
echo "  systemctl enable --now cubefs-mount@data"
# metrics exporter:开机自启 + 立即启动(Prometheus scrape <node>:9970/metrics)
systemctl enable --now cubefs-client-exporter 2>/dev/null || true
echo "[cubefs-kmod] metrics exporter 已启动(:9970/metrics);非默认卷需:"
echo "  echo 'EXPORTER_OPTS=\"-vol=<vol>\"' > /etc/cubefs/exporter.conf && systemctl restart cubefs-client-exporter"
EOF

# 6. prerm: 停挂载服务 + umount + 卸载模块
cat > "$P/DEBIAN/prerm" <<'EOF'
#!/bin/sh
set -e
if [ -d /run/systemd/system ]; then
  systemctl stop 'cubefs-mount@*' 2>/dev/null || true
  systemctl disable --now cubefs-client-exporter 2>/dev/null || true
fi
umount -t cubefs -a 2>/dev/null || true
rmmod cubefs 2>/dev/null || true
EOF
chmod 755 "$P/DEBIAN/postinst" "$P/DEBIAN/prerm"

# 7. 打包
mkdir -p "$OUT"
DEB="$OUT/cubefs-kmod_${PKGVER}_${KVER}_amd64.deb"
dpkg-deb --build "$P" "$DEB" >/dev/null
rm -rf "$(dirname "$P")"

echo "==> 生成 $DEB"
dpkg-deb -I "$DEB" | grep -E "Package:|Version:|Architecture:"
echo "==> 安装: dpkg -i $DEB"
