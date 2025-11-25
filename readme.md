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


-- jenkins script for poweshell .
 #!/bin/bash

set -e
echo "--- 1. BUILDING AUTOMATION RUNNER IMAGE (FINAL ATTEMPT) ---"
# Force build to ensure the file is copied into the image
docker build --no-cache -t schema-sync-runner-final .

echo "--- 2. EXECUTING SCHEMA SYNCHRONIZATION (MOUNT FIX) ---"
# CRITICAL FIX: We are removing the volume mount for the code (-v "$(pwd)":/app)
# This relies entirely on the file being built INTO the image.
# We ADD A NEW MOUNT for the Flyway SRC directory ONLY so it can write the SQL file.
docker run --rm \
  --entrypoint "/bin/bash" \
  --network=flyway-project1_common-network \
  -v "$(pwd)/src":/app/src \
  schema-sync-runner-final \
  -c "echo '--- Starting Python Script from Image... ---' && python /app/auto_migrate_schema.py"

echo "--- 3. PIPELINE COMPLETE ---"