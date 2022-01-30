FROM python:3.9.1

RUN python -m pip install --upgrade pip

WORKDIR /app

COPY requirements.txt requirements.txt
COPY pipeline.py pipeline.py

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "pipeline.py",  ]