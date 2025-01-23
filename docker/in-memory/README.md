# Polaris In-Memory

1. Build the Polaris image if it's not already present locally:

    ```shell
    ./gradlew clean :polaris-quarkus-server:assemble -Dquarkus.container-image.build=true
    ```

2. Start the docker compose group using either podman or docker:

    ```shell
    podman compose -f docker/in-memory/docker-compose.yml up
    ```
    ```shell
    docker compose -f docker/in-memory/docker-compose.yml up
    ```

3. To access Polaris from the host machine, first request an access token:

    ```shell
    export POLARIS_TOKEN=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
       --resolve polaris:8181:127.0.0.1 \
       --user root:s3cr3t \
       -d 'grant_type=client_credentials' \
       -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)
    ```

4. Then, use the access token in the Authorization header when accessing Polaris:

    ```shell
    curl -v http://127.0.0.1:8181/api/management/v1/principal-roles -H "Authorization: Bearer $POLARIS_TOKEN"
    ```
