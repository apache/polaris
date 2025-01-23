# Polaris with EclipseLink, Postgres and Spark SQL

1. If such an image is not already present, build the Polaris image with support for EclipseLink and
   the Postgres JDBC driver:

    ```shell
    ./gradlew clean :polaris-quarkus-server:assemble :polaris-quarkus-admin:assemble \
       -PeclipseLinkDeps=org.postgresql:postgresql:42.7.4 \
       -Dquarkus.container-image.build=true
    ```

2. Start the docker compose group using either podman or docker:

    ```shell
    podman compose -f docker/eclipselink/docker-compose.yml up
    docker compose -f docker/eclipselink/docker-compose.yml up
    ```

3. Using spark-sql: attach to the running spark-sql container:

    ```shell
    podman attach $(podman ps -q --filter name=spark-sql)
    docker attach $(docker ps -q --filter name=spark-sql)
    ```

   You may not see Spark's prompt immediately, type ENTER to see it. A few commands that you can try:

    ```sql
     CREATE NAMESPACE polaris.ns1;
     USE polaris.ns1;
     CREATE TABLE table1 (id int, name string);
     INSERT INTO table1 VALUES (1, 'a');
     SELECT * FROM table1;
    ```

4. To access Polaris from the host machine, first request an access token:

    ```shell
    export POLARIS_TOKEN=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
       --resolve polaris:8181:127.0.0.1 \
       --user root:s3cr3t \
       -d 'grant_type=client_credentials' \
       -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)
    ```

5. Then, use the access token in the Authorization header when accessing Polaris:

    ```shell
    curl -v http://127.0.0.1:8181/api/management/v1/principal-roles -H "Authorization: Bearer $POLARIS_TOKEN"
    curl -v http://127.0.0.1:8181/api/catalog/v1/config?warehouse=polaris_demo -H "Authorization: Bearer $POLARIS_TOKEN"
    ```
