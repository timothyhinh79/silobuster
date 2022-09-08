#Deriving the latest base image
FROM python:3.10-slim

RUN apt update && \
  DEBIAN_FRONTEND=noninteractive apt install -y \
    postgresql-client-common postgresql-common python3-dev build-essential bash
    
RUN apt install -y libpq-dev

RUN mkdir /app

WORKDIR /app

COPY requirements.txt ./
RUN python -m venv venv
RUN ./venv/bin/pip install wheel
RUN ./venv/bin/pip install -r requirements.txt

COPY sanitize_contact_info.py entrypoint.bash ./

ENTRYPOINT ["/bin/bash", "/app/entrypoint.bash"]