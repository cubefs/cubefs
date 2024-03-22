#/bin/bash!

wget storage.jd.local/ops-upload/cube_torch-0.3-py3-none-any.whl -O cube_torch-0.3-py3-none-any.whl
pip3 uninstall -y cube_torch
pip3 install cube_torch-0.3-py3-none-any.whl
\rm -f cube_torch-0.3-py3-none-any.whl
