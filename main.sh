#!/bin/bash
echo __Starting Execution...

echo ...Installing Requirements...
pip install -r requirements.txt

echo ...Initiating Docker containers...
docker-compose up -d --build

echo ...Executing Unit Testing...
pytest tests/*

echo Executing SALES_DATA Script...
python workflows/sales_data.py
echo ...SALES_DATA Script Completed!

echo Executing WEATHER_API Script...
python workflows/weather_api.py
echo ...WEATHER_API Script Completed!

echo Executing WEBSITE_LOGS Script...
python workflows/website_logs.py
echo ...WEBSITE_LOGS Script Completed!

echo __Execution Completed!