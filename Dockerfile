# Dockerfile
FROM maven:3.9.5-eclipse-temurin-17-alpine AS build

# Install Python and necessary packages
RUN apk add --no-cache python3 py3-pip
RUN pip install migra 'psycopg2-binary>=2.9.9'

# Set environment variables
ENV LANG=C.UTF-8

# Crucial: Set the working directory
WORKDIR /app

# Copy the script directly from the build context root to the /app directory.
COPY auto_migrate_schema.py /app/
COPY pom.xml /app/
COPY src /app/src

# Ensure permissions are correct
RUN chmod +x /app/auto_migrate_schema.py