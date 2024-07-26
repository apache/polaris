#!/bin/bash

if [ ! -d ~/polaris-venv ]; then
  python3 -m venv ~/polaris-venv
fi

. ~/polaris-venv/bin/activate

pip install poetry==1.5.0

cd client/python
python3 -m poetry install
deactivate