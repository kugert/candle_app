FROM python:3.7-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /code
COPY . /code/
RUN pip install -r requirements.txt
CMD ['python', 'main.py']
