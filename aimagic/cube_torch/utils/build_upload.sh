#!/bin/bash

cd ../

python3 setup.py bdist_wheel
uploadToJss -f dist/cube_torch-0.2-py3-none-any.whl -k cube_torch-0.2-py3-none-any.whl
\rm -rf build cube_torch.egg-info dist cube_torch/__pycache__