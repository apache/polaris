# Apache Polaris Quickstart with File-Based Backend

This quickstart guide helps you get Apache Polaris up and running quickly using Docker Compose with a simple file-based backend.

## Overview

This setup provides:
- **Apache Polaris** server with file-based storage
- **Persistent storage** using Docker volumes
- **Simple authentication** with default credentials

## Prerequisites

- Docker and Docker Compose installed on your machine

## Running the Quickstart

You can run this from anywhere! Just download the `docker-compose.yml` file or run it directly:

```bash
docker compose -f getting-started/quickstart/docker-compose.yml up
```

This will:
1. Pull the latest Apache Polaris Docker image
2. Start the Polaris server on ports 8181 (API) and 8182 (management)
3. Automatically create a catalog named `quickstart_catalog`
4. Create a user principal `quickstart_user` with full access to the catalog
5. Display the credentials you need to get started
