FROM python:3.9

WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt requirements.txt

# Install dependencies
RUN pip install -r requirements.txt

# Copy all project files into the container
COPY . .

# Run the Flask app
CMD ["python", "flask_api.py"]
