#!/usr/bin/env bash
set -e

# Debug info
echo "Current working directory: $(pwd)"
echo "Directory contents:"
ls -la /app/
echo "Movielens directory contents:"
ls -la /app/movielens/

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until nc -z $SUPABASE_DB_HOST $SUPABASE_DB_PORT; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL is up - executing command"

# Run the data loading script if environment variable is set
if [ "$LOAD_DATA" = "false" ]; then
  echo "Loading data into PostgreSQL..."
  python load_supabase.py
  echo "Data loading complete"
fi

# Start the Flask app
echo "Starting Flask API..."
python supabase_api.py