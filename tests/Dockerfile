FROM mcr.microsoft.com/playwright/python:v1.49.1-noble
WORKDIR /tests
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# raw output goes to /results/pytest_output.txt as well as stdout
CMD ["bash", "-c", "set -o pipefail; pytest -v -rA -s 2>&1 | tee /results/pytest_output.txt"]
