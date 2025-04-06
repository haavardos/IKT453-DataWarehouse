FROM python:3.9

# Create a working directory
WORKDIR /app

# Copy your requirements file
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Flask app code
COPY neo4j_api.py /app/

EXPOSE 6060

CMD [ "python", "neo4j_api.py" ]
