# Getting Started with Apache Polaris: Minio S3, Governance with Spark & Trino (Read-Only)

This example demonstrates setting up Apache Polaris to manage an Iceberg data lake in Minio S3, focusing on governance.
Polaris uses Postgres for its metadata. Spark SQL is configured for read/write access to create and populate Iceberg tables. Trino is configured for **strict read-only access** to query these tables. Access control is enforced by Polaris, with underlying S3 permissions managed by Minio.

**Prerequisites:**
* Docker and Docker Compose.
* `jq` installed on your host machine.
* Apache Polaris images (`apache/polaris-admin-tool:postgres-latest`, `apache/polaris:postgres-latest`) built from source with JDBC support, tagged as `postgres-latest`. 

Run 

```shell
    ./gradlew \
       :polaris-quarkus-server:assemble \
       :polaris-quarkus-server:quarkusAppPartsBuild --rerun \
       :polaris-quarkus-admin:assemble \
       :polaris-quarkus-admin:quarkusAppPartsBuild --rerun \
       -Dquarkus.container-image.tag=postgres-latest \
       -Dquarkus.container-image.build=true
```

**Security Overview:**
* **Minio (S3 Storage):**
    * `polaris_s3_user` (R/W): Used by Polaris service for warehouse management.
    * `spark_minio_s3_user` (R/W): Used by Spark engine for data R/W operations.
    * `trino_minio_s3_user` (R/O): Used by Trino engine for data read operations.
* **Polaris (Catalog & Governance):**
    * `root` user: Admin access to Polaris.
    * `spark_app_client`: Polaris client ID for Spark, assigned `polaris_spark_role` (R/W permissions on `minio_catalog.ns_governed`).
    * `trino_app_client`: Polaris client ID for Trino, assigned `polaris_trino_role` (R/O permissions on `minio_catalog.ns_governed`).

**Setup and Execution:**

1.  **Environment Variables (Optional):**
    Create a `.env` file in this directory (`getting-started/minio/.env`) to customize credentials and ports. Example:
    ```env
    # Minio Settings
    MINIO_ROOT_USER=minioadmin
    MINIO_ROOT_PASSWORD=minioadmin
    MINIO_API_PORT=9000
    MINIO_CONSOLE_PORT=9001

    # Minio S3 User Credentials (used by services, created by mc)
    POLARIS_S3_USER=polaris_s3_user
    POLARIS_S3_PASSWORD=polaris_s3_password_val
    SPARK_MINIO_S3_USER=spark_minio_s3_user
    SPARK_MINIO_S3_PASSWORD=spark_minio_s3_password_val
    TRINO_MINIO_S3_USER=trino_minio_s3_user
    TRINO_MINIO_S3_PASSWORD=trino_minio_s3_password_val

    # Polaris Client Credentials (for Spark & Trino to auth to Polaris, created by bootstrap)
    SPARK_POLARIS_CLIENT_ID=spark_app_client
    SPARK_POLARIS_CLIENT_SECRET=spark_client_secret_val
    TRINO_POLARIS_CLIENT_ID=trino_app_client
    TRINO_POLARIS_CLIENT_SECRET=trino_client_secret_val

    # Ports
    POSTGRES_MINIO_PORT=5433
    POLARIS_MINIO_API_PORT=8183
    POLARIS_MINIO_MGMT_PORT=8184
    SPARK_UI_MINIO_START_PORT=4050
    # SPARK_UI_MINIO_END_PORT=4055 # Not strictly needed if using start port only for mapping range
    TRINO_MINIO_PORT=8083
    ```

2.  **Ensure Scripts are Executable:**
    ```bash
    chmod +x getting-started/minio/minio-config/setup-minio.sh
    chmod +x getting-started/minio/polaris-config/create-catalog-minio.sh
    chmod +x getting-started/minio/polaris-config/setup-polaris-governance.sh
    ```

3.  **Start Services:**
    Navigate to `getting-started/minio` and run:
    ```shell
    docker compose up -d --build
    ```
    This will start all services, including Minio setup, Polaris bootstrap (creating `root`, `spark_app_client`, `trino_app_client` principals), Polaris catalog creation, and Polaris governance setup (creating roles and assigning grants). Check logs with `docker compose logs -f`.

4.  **Access Minio Console:**
    `http://localhost:${MINIO_CONSOLE_PORT:-9001}` (default: `minioadmin`/`minioadmin`). Verify `polaris-bucket`.

5.  **Using Spark SQL (Read/Write Access):**
    Attach to Spark: `docker attach spark-sql-minio-gov` (Press ENTER for prompt).
    The default catalog is `polaris_minio_gov`.
    ```sql
    -- Create a namespace governed by Polaris policies
    CREATE NAMESPACE IF NOT EXISTS ns_governed
      COMMENT 'Namespace for governed data access'
      LOCATION 's3a://polaris-bucket/iceberg_warehouse/minio_catalog/ns_governed/'; -- Optional but good practice

    USE ns_governed;

    -- Create an Iceberg table
    CREATE TABLE IF NOT EXISTS my_gov_table (id INT, name STRING, value DOUBLE)
    USING iceberg
    COMMENT 'Governed table for Spark R/W and Trino R/O demo'
    TBLPROPERTIES ('format-version'='2');

    -- Insert data
    INSERT INTO my_gov_table VALUES (1, 'SparkRecordOne', 10.1), (2, 'SparkRecordTwo', 20.2);

    -- Select data
    SELECT * FROM my_gov_table ORDER BY id;
    -- Expected: Shows inserted records.
    ```

6.  **Using Trino CLI (Strict Read-Only Access):**
    Access Trino CLI: `docker exec -it minio-trino-gov trino`
    The Polaris catalog is mapped to `iceberg` in Trino.
    ```sql
    SHOW CATALOGS;
    -- Expected: iceberg, system, ...

    SHOW SCHEMAS FROM iceberg;
    -- Expected: information_schema, ns_governed

    SHOW TABLES FROM iceberg.ns_governed;
    -- Expected: my_gov_table

    DESCRIBE iceberg.ns_governed.my_gov_table;
    -- Expected: Schema of my_gov_table

    SELECT * FROM iceberg.ns_governed.my_gov_table ORDER BY id;
    -- Expected: Shows records inserted by Spark.

    -- Test Read-Only: Attempt to create a table (SHOULD FAIL)
    -- CREATE TABLE iceberg.ns_governed.trino_test_table (id INT) WITH (location = 's3a://polaris-bucket/iceberg_warehouse/minio_catalog/ns_governed/trino_test_table/');
    -- Expected: Error from Polaris indicating permission denied for CREATE_TABLE.

    -- Test Read-Only: Attempt to insert data (SHOULD FAIL)
    -- INSERT INTO iceberg.ns_governed.my_gov_table VALUES (3, 'TrinoRecord', 30.3);
    -- Expected: Error, as Trino's Polaris role and Minio S3 user are read-only.
    ```

7.  **Accessing Polaris API (Optional):**
    Get token for `trino_app_client` (should have limited scope):
    ```shell
    export POLARIS_API_ENDPOINT="http://localhost:${POLARIS_MINIO_API_PORT:-8183}"
    export TRINO_APP_TOKEN=$(curl -s "${POLARIS_API_ENDPOINT}/api/catalog/v1/oauth/tokens" \
        --user "${TRINO_POLARIS_CLIENT_ID:-trino_app_client}:${TRINO_POLARIS_CLIENT_SECRET:-trino_client_secret_val}" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d 'grant_type=client_credentials' \
        -d 'realmName=POLARIS_MINIO_REALM' | jq -r .access_token)
    echo "Trino App Token: $TRINO_APP_TOKEN"

    # Try to list tables using Trino's token
    curl -v "${POLARIS_API_ENDPOINT}/api/catalog/v1/minio_catalog/namespaces/ns_governed/tables" -H "Authorization: Bearer $TRINO_APP_TOKEN"
    # This should succeed.
    ```

8.  **Cleanup:**
    ```shell
    docker compose down -v
    ```

This set of scripts and configurations should enforce the desired access controls, with Trino having strictly read-only capabilities.