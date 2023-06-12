#!/bin/bash

if [ "$1" == "--bash" ]; then
    # echo "Running with bash"
    exec /bin/bash
else
    # echo "Running default behavior"

# Ensure required environment variables are set
# if [[ -z "${NEXUS_TMDB_AWS_ACCESS_KEY_ID}" || -z "${NEXUS_TMDB_AWS_SECRET_ACCESS_KEY}" || -z "${NEXUS_TMDB_S3_SERVER_PATH}" ]]; then
#   echo "Error: Required environment variables (NEXUS_TMDB_AWS_ACCESS_KEY_ID, NEXUS_TMDB_AWS_SECRET_ACCESS_KEY, NEXUS_TMDB_S3_SERVER_PATH) not set."
#   exit 1
# fi

export CURRENT_UID=$(id -u)
export CURRENT_GID=$(id -g)

sudo echo "${NEXUS_TMDB_AWS_ACCESS_KEY_ID}:${NEXUS_TMDB_AWS_SECRET_ACCESS_KEY}" > ~/.s3fs.passwd

# sudo s3fs ${NEXUS_TMDB_S3_BUCKET_NAME}:/Archive /opt/python_scripts/tmdb-data-retriever/data/Archive -o passwd_file=~/.s3fs.passwd -o url=$NEXUS_TMDB_S3_SERVER_PATH -o use_path_request_style -o uid=$CURRENT_UID,gid=$CURRENT_GID,umask=0077,allow_other -o nonempty
# sudo s3fs ${NEXUS_TMDB_S3_BUCKET_NAME}:/Logs /opt/python_scripts/tmdb-data-retriever/data/Logs -o passwd_file=~/.s3fs.passwd -o url=$NEXUS_TMDB_S3_SERVER_PATH -o use_path_request_style -o uid=$CURRENT_UID,gid=$CURRENT_GID,umask=0077,allow_other -o nonempty
sudo s3fs ${NEXUS_TMDB_S3_BUCKET_NAME}:/Upload /opt/python_scripts/tmdb-data-retriever/data/Upload -o passwd_file=~/.s3fs.passwd -o url=$NEXUS_TMDB_S3_SERVER_PATH -o use_path_request_style -o uid=$CURRENT_UID,gid=$CURRENT_GID,umask=0077,allow_other -o nonempty
sudo s3fs ${NEXUS_TMDB_S3_BUCKET_NAME}:/Images /opt/python_scripts/tmdb-data-retriever/data/Images -o passwd_file=~/.s3fs.passwd -o url=$NEXUS_TMDB_S3_SERVER_PATH -o use_path_request_style -o uid=$CURRENT_UID,gid=$CURRENT_GID,umask=0077,allow_other -o nonempty
sudo s3fs ${NEXUS_TMDB_S3_BUCKET_NAME}:/Config /opt/python_scripts/tmdb-data-retriever/data/Config -o passwd_file=~/.s3fs.passwd -o url=$NEXUS_TMDB_S3_SERVER_PATH -o use_path_request_style -o uid=$CURRENT_UID,gid=$CURRENT_GID,umask=0077,allow_other -o nonempty

# Run the specified command
# exec "$@"

# tail -f /dev/null
# bash

if [ "$1" == "--bash" ]; then
    # echo "Running with bash"
    cd /opt/python_scripts/tmdb-data-retriever/src/tmdb_data_retriever
    exec /bin/bash
else
    # echo "Running default behavior"
    cd /opt/python_scripts/tmdb-data-retriever/src/tmdb_data_retriever
    python main.py api_listener -host '0.0.0.0' -p 5002
fi
