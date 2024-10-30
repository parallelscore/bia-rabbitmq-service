# Use an official Python runtime as a parent image
FROM python:3.10-slim

ARG RABBITMQ_URL
ARG REDIS_HOST
ARG REDIS_PORT
ARG REDIS_DB
ARG REDIS_PASSWORD
ARG AI_SERVICE_BASE_URL
ARG ERROR_QUEUE
ARG AI_ANALYSIS_QUEUE
ARG BACKEND_PUBLISH_QUEUE

# Set environment variables
ENV RABBITMQ_URL=$RABBITMQ_URL
ENV REDIS_HOST=$REDIS_HOST
ENV REDIS_PORT=$REDIS_PORT
ENV REDIS_DB=$REDIS_DB
ENV REDIS_PASSWORD=$REDIS_PASSWORD
ENV AI_SERVICE_BASE_URL=$AI_SERVICE_BASE_URL
ENV AI_ANALYSIS_QUEUE=$AI_ANALYSIS_QUEUE
ENV BACKEND_PUBLISH_QUEUE=$BACKEND_PUBLISH_QUEUE

# Set the working directory in the container to /app
WORKDIR /home

# Create api directory inside /app
RUN mkdir /home/app

# Add the module directory contents into the container at /app/api
ADD ./app /home/app

# Copy the requirements file into the container at /app
ADD ./requirements.txt /home

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run app.py when the container launches
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
