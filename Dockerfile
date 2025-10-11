FROM python:3.9

WORKDIR /stock-bars-data-engineering-project

COPY etl_project ./etl_project

COPY etl_project_tests ./etl_project_tests

COPY requirements.txt .

RUN pip install -r requirements.txt 

CMD ["python", "-m", "etl_project.pipelines.stock_bars"]