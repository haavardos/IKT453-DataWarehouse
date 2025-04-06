FROM python:3.9

# Create a working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Flask app code
COPY supabase_api.py /app/

EXPOSE 5050

CMD ["python", "supabase_api.py"]