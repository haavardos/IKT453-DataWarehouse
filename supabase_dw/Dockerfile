FROM python:3.9

# Create a working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy Flask app code
COPY supabase_api.py /app/
COPY load_supabase.py /app/

# Expose the port the app runs on
EXPOSE 5050

# Install netcat for the health check
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Copy and make the startup script executable
COPY startup.sh /app/startup.sh
RUN chmod +x /app/startup.sh

# Run the startup script
CMD ["/app/startup.sh"]