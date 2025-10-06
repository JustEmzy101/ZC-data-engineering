FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 
WORKDIR /app
COPY basic_ingestion.py basic_ingestion.py

ENTRYPOINT [ "python","basic_ingestion.py" ]