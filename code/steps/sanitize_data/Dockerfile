#Deriving the latest base image
FROM python:3.10-slim

RUN apt update && \
    DEBIAN_FRONTEND=noninteractive apt install -y \
      postgresql-client-common postgresql-common libpq-dev \
      python3-dev build-essential bash

RUN mkdir /app
WORKDIR /app

# installing packages in requirements.txt in virtual environment
COPY requirements.txt ./
RUN python -m venv venv
RUN ./venv/bin/pip install wheel
RUN ./venv/bin/pip install -r requirements.txt

# copying sanitization code into /app
COPY run_sanitization.py entrypoint.bash ./
COPY classes classes/
COPY sanitization_code sanitization_code/

# run run_sanitization.py within virtual environment
ENTRYPOINT ["/bin/bash", "/app/entrypoint.bash"]
# ENTRYPOINT ["tail", "-f", "/dev/null"] # to make container persist for debugging