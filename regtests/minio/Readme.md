<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# MiniIO Secured
## Minio and secured buckets with TLS self-signed / custom AC

To be able to test Polaris with buckets in TLS under custom AC or self-signed certificate

## MiniIO generate self-signed certificates designed for docker-compose setup

- Download minio certificate generator : https://github.com/minio/certgen
- ```./certgen -host "localhost,minio,*"```
- put them in ./certs and ./certs/CAs
- they will be mounted in default minio container placeholder

## Test minIO secured TLS buckets from self-signed certificate with AWS CLI 
- ```aws s3 ls s3:// --recursive --endpoint-url=https://localhost:9000 --no-verify-ssl```
- ```aws s3 ls s3:// --recursive --endpoint-url=https://localhost:9000 --ca-bundle=./certs/public.crt```

## add to java cacerts only the public.crt as an AC
- ```sudo keytool -import -trustcacerts -cacerts -storepass changeit -noprompt -alias minio -file ./certs/public.crt```
- ```keytool -list -cacerts -alias minio -storepass changeit```

## remove from java cacerts the public.crt
- ```sudo keytool -delete -trustcacerts -cacerts -storepass changeit -noprompt -alias minio```
- ```keytool -list -cacerts -alias minio -storepass changeit```
