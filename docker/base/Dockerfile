# FROM ubuntu:latest
# FROM ubuntu:20.04
FROM tiangolo/uwsgi-nginx

RUN apt update && \
    apt install -y software-properties-common build-essential checkinstall sudo nano

RUN apt update && \
    apt install -y python3-pip git dos2unix

RUN pip3 install --upgrade pip

RUN alias python3=python3.8

RUN pip3 install \
    'pandas>=1.5.3,<2.0.0' \
    'sqlalchemy>=2.0.4,<3.0.0' \
    'psycopg2-binary>=2.9.5,<3.0.0' \
    'configparser>=5.3.0,<6.0.0' \
    'dateparser>=1.1.8,<2.0.0' \
    'keyring>=23.13.1,<24.0.0' \
    'flask>=2.2.5,<3.0.0' \
    'waitress>=2.1.2,<3.0.0'
    #'nexus-utilities'

# RUN mkdir /opt/python_scripts

# RUN mkdir /opt/python_scripts/tmdb-data-retriever

# RUN echo "cd ../.. && $pwd" > /tmp/print_path.sh

COPY . /opt/python_scripts/tmdb-data-retriever

RUN pip3 install --upgrade 'nexus-utilities >=0.6.0,<1.0.0'

WORKDIR /opt/python_scripts/tmdb-data-retriever

# This makes relative imports work through the application
RUN pip3 install -e .

RUN echo "alias ls='ls -1 -a'" >> /root/.bashrc

ENTRYPOINT ["/bin/bash"]