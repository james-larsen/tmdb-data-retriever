FROM nexus-tmdb-data-retriever-base:latest

RUN rm -rf /opt/python_scripts/tmdb-data-retriever/config

COPY ./docker/local/config /opt/python_scripts/tmdb-data-retriever/config

COPY ./docker/local/entrypoint.sh /opt/python_scripts/tmdb-data-retriever

RUN dos2unix /opt/python_scripts/tmdb-data-retriever/entrypoint.sh

RUN chmod +x /opt/python_scripts/tmdb-data-retriever/entrypoint.sh

ENTRYPOINT ["/opt/python_scripts/tmdb-data-retriever/entrypoint.sh"]

# ENTRYPOINT ["/bin/bash"]