#!/bin/bash

# Get version from Gradle
VERSION=$(./gradlew properties -q | grep "version:" | awk '{print $2}')

# Set paths
ADMIN_DIST="quarkus/admin/build/distributions/polaris-quarkus-admin-${VERSION}.tgz"
SERVER_DIST="quarkus/server/build/distributions/polaris-quarkus-server-${VERSION}.tgz"
OUTPUT_DIR="polaris-quarkus-combined-${VERSION}"

# Clean up any existing directories
rm -rf temp temp2 "${OUTPUT_DIR}"
mkdir -p temp temp2

# Extract both distributions
cd temp
tar xf "../${ADMIN_DIST}"
cd ../temp2
tar xf "../${SERVER_DIST}"
cd ..

# Create combined directory structure
mkdir -p "${OUTPUT_DIR}"

# Copy shared files
cp temp/polaris-quarkus-admin-${VERSION}/LICENSE "${OUTPUT_DIR}/"
cp temp/polaris-quarkus-admin-${VERSION}/NOTICE "${OUTPUT_DIR}/"
cp temp/polaris-quarkus-admin-${VERSION}/README.md "${OUTPUT_DIR}/"

# Create admin and server directories
mkdir -p "${OUTPUT_DIR}/admin" "${OUTPUT_DIR}/server"

# Copy the complete application structure for admin
cp -r temp/polaris-quarkus-admin-${VERSION}/{app,lib,quarkus,quarkus-run.jar,quarkus-app-dependencies.txt} "${OUTPUT_DIR}/admin/"

# Copy the complete application structure for server
cp -r temp2/polaris-quarkus-server-${VERSION}/{app,lib,quarkus,quarkus-run.jar,quarkus-app-dependencies.txt} "${OUTPUT_DIR}/server/"

# Create combined run script
cat > "${OUTPUT_DIR}/run.sh" << 'EOL'
#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Default to server if no component specified
COMPONENT=${1:-server}

if [ "$COMPONENT" != "server" ] && [ "$COMPONENT" != "admin" ]; then
    echo "Usage: $0 [server|admin] [additional arguments...]"
    exit 1
fi

# Shift off the first argument so $@ contains remaining args
shift

# Common JVM options
JAVA_OPTS="${JAVA_OPTS} -Dquarkus.http.host=0.0.0.0"

if [ "$COMPONENT" = "server" ]; then
    echo "Starting Polaris Server..."
    cd server
    java ${JAVA_OPTS} -jar quarkus-run.jar "$@"
else
    echo "Starting Polaris Admin Tool..."
    cd admin
    java ${JAVA_OPTS} -jar quarkus-run.jar "$@"
fi
EOL

chmod +x "${OUTPUT_DIR}/run.sh"

# Create combined dependencies file for reference
cat "${OUTPUT_DIR}/admin/quarkus-app-dependencies.txt" "${OUTPUT_DIR}/server/quarkus-app-dependencies.txt" | sort | uniq > "${OUTPUT_DIR}/quarkus-app-dependencies.txt"

# Create tarball
tar czf "polaris-quarkus-combined-${VERSION}.tgz" "${OUTPUT_DIR}"

# Clean up
rm -rf temp temp2 "${OUTPUT_DIR}"

echo "Created combined distribution: polaris-quarkus-combined-${VERSION}.tgz" 