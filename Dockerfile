FROM python:3.10

WORKDIR /code/src

# Install OS and Python required package
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Set display port to avoid crash
ENV DISPLAY=:99

