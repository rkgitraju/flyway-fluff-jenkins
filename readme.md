this is setup of runing migration autmatically using migra and flyawy. migra is geenrating multiple SQL files as per table changes.

-- window : set timezone.
$env:MAVEN_OPTS="-Duser.timezone=Asia/Kolkata"
$env:PYTHONWARNINGS = "ignore"

-- run single profile
mvn clean compile flyway:migrate -Pdb1-local

-- repair flyway 
mvn flyway:repair -Pdb2-local

-- compare schema
migra --unsafe postgresql://airflow:airflow@localhost:5432/db2 postgresql://airflow:airflow@localhost:5432/db1


-- setup with jenkins.
1. Dockerfile
2. docker-compose.yml

-- run compose and build 
docker-compose up -d --build

-- upload your project on git. 
-- add new item as free style
-- set github link
-- Check Delete workspace before build starts to ensure you are starting clean every time.
-- add new step and paste below script in command shell

-- jenkins script for poweshell .
#!/bin/bash
set -e

# 1. Enter the directory mapped in your docker-compose
cd /home/jenkins/workspace/flyway-project1

echo "--- 2. BUILDING MIGRATION IMAGE ---"
# This builds the image using 'auto_migrate_schema.py' inside the container
docker build --no-cache -t schema-sync-runner-final .

echo "--- 3. EXECUTING SCHEMA SYNCHRONIZATION ---"
# Use the correct network name and the auto_migrate_schema.py filename
echo "yes" | docker run --rm \
  --network=flyway-project1_common-network \
  -v "$(pwd)/src":/app/src \
  schema-sync-runner-final \
  python3 auto_migrate_schema.py