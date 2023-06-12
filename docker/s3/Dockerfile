FROM nexus-tmdb-data-retriever-base:latest

RUN apt update && \
    apt install -y s3fs fuse

RUN rm -rf /opt/python_scripts/tmdb-data-retriever/config

# RUN git clone https://github.com/james-larsen/tmdb-data-retriever.git /opt/python_scripts/tmdb-data-retriever
# RUN mkdir -p /opt/python_scripts/tmdb-data-retriever/data
# RUN mkdir -p /opt/python_scripts/tmdb-data-retriever/data/Archive
# RUN mkdir -p /opt/python_scripts/tmdb-data-retriever/data/Logs
# RUN mkdir -p /opt/python_scripts/tmdb-data-retriever/data/Upload
# RUN mkdir -p /opt/python_scripts/tmdb-data-retriever/data/config

COPY ./docker/s3/config /opt/python_scripts/tmdb-data-retriever/config

COPY ./docker/s3/entrypoint.sh /opt/python_scripts/tmdb-data-retriever

RUN dos2unix /opt/python_scripts/tmdb-data-retriever/entrypoint.sh

# RUN pip3 install --upgrade 'nexus-utilities >=0.6.0,<1.0.0'

RUN touch ~/.s3fs.passwd

RUN chmod 600 ~/.s3fs.passwd

RUN chmod +x /opt/python_scripts/tmdb-data-retriever/entrypoint.sh

ENTRYPOINT ["/opt/python_scripts/tmdb-data-retriever/entrypoint.sh"]

# ENTRYPOINT ["/bin/bash"]