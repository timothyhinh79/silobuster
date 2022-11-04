#Deriving the latest base image
FROM python:3.10-slim

RUN apt update && \
    DEBIAN_FRONTEND=noninteractive apt install -y \
      postgresql-client-common postgresql-common libpq-dev \
      python3-dev build-essential bash && \
    YES=yes sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh && \
    apt install -y postgresql-client

RUN mkdir /app
WORKDIR /app

# installing packages in requirements.txt
COPY requirements.txt ./
RUN pip install -r requirements.txt

# copying sanitization code into /app
COPY run_sanitization.py entrypoint.bash ./
COPY classes classes/
COPY sanitization_code sanitization_code/

# creating folder to store temporary dump files for transfering tables from source to destination databases
RUN mkdir tmp

# run run_sanitization.py
ENTRYPOINT ["/bin/bash", "/app/entrypoint.bash"]
# CMD ["tail", "-f", "/dev/null"] # to make container persist for debugging