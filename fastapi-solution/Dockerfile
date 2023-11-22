FROM python:3.10

WORKDIR /opt/api

RUN groupadd -r web && useradd -d /opt/api -r -g web web \
    && chown web:web -R /opt/api

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY entrypoint.sh entrypoint.sh
COPY requirements/base.txt requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY ./src .

USER web

ENTRYPOINT ["sh", "entrypoint.sh"]
