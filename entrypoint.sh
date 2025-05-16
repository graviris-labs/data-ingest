#!/bin/bash

# Navigate to the application directory
cd /app

# Create necessary directories if they don't exist
mkdir -p data/db
mkdir -p logs

echo "Starting wildfire data scraper with hourly schedule"

# Run the scheduler script
python src/scheduler.py