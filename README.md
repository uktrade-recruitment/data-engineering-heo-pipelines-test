# Weather Pipeline Technical Test

## Getting Started

This technical test involves working with a Python-based ETL pipeline that processes weather data.

### Prerequisites

- Docker installed on your system
- Run the docker build command (step 1)

### Running the Test Environment

1. Build the Docker image:
docker build -t weather-pipeline-test .

2. Run the container:
docker run -it weather-pipeline-test

3. The test environment will:
   - Run the pipeline with sample data
   - Execute the existing tests
   - Provide you with a shell to work on the tasks

4. Complete the tasks outlined in the test instructions below, using the shell provided in the container.

5. To re-run the pipeline after making changes:
python src/main.py

6. To re-run the tests:
python -m unittest discover -s tests