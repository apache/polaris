# This could totally be an azure pipeline. 
VERSION=$(cat ../version.txt)

docker tag apache/polaris:${VERSION} uchimera.azurecr.io/apache/polaris:${VERSION}
docker tag apache/polaris-admin-tool:${VERSION} uchimera.azurecr.io/apache/polaris-admin-tool:${VERSION}
az acr login -n uchimera
if [ $? -ne 0 ]; then
  echo "try an az login"
  exit 1
fi
docker push uchimera.azurecr.io/apache/polaris:${VERSION}
docker push uchimera.azurecr.io/apache/polaris-admin-tool:${VERSION}

cd helm
helm package ./polaris
mv *.tgz ../