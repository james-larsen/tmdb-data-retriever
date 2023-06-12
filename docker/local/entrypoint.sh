#!/bin/bash

if [ "$1" == "--bash" ]; then
    # echo "Running with bash"
    cd /opt/python_scripts/tmdb-data-retriever/src/tmdb_data_retriever
    exec /bin/bash
else
    # echo "Running default behavior"
    cd /opt/python_scripts/tmdb-data-retriever/src/tmdb_data_retriever
    python main.py api_listener -host '0.0.0.0' -p 5002
fi
