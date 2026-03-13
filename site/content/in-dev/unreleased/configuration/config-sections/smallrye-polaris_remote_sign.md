---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: smallrye-polaris_remote_sign
build:
  list: never
  render: never
---

Configuration for remote sign.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.remote-sign.keys.`_`<name>`_ |  | `string` | Map of key IDs to their base64-encoded secret keys. These keys are used for encrypting and  decrypting JWEs.   <br><br>Each key must have the correct length for the selected encryption method. Multiple keys can  be configured to enable smooth key rotation - old keys can still decrypt existing tokens while  new tokens are encrypted with the current key.   <br><br>Multiple keys can be configured to enable smooth key rotation. The (`#currentKeyId()`) specifies which key is used for encryption, while all configured keys can be used for  decryption.   <br><br>For production use, consider using (`#keyFiles()`) instead to avoid exposing secrets in  configuration.   <br><br>Example of configuration:   <br><br>```<br> polaris.remote-signing.keys.key1=base64EncodedKey1<br> polaris.remote-signing.keys.key2=base64EncodedKey2<br> ```<br><br> |
| `polaris.remote-sign.key-files.`_`<name>`_ |  | `path` | Map of key IDs to file paths containing base64-encoded secret keys. These keys are used for  encrypting and decrypting JWEs.   <br><br>Each file should contain a single base64-encoded key that is exactly 32 bytes (256 bits)  when decoded. The file content is read at startup and trimmed of leading/trailing whitespace.   <br><br>Multiple keys can be configured to enable smooth key rotation. The (`#currentKeyId()`) specifies which key is used for encryption, while all configured keys can be used for  decryption.   <br><br>This is the preferred method for production deployments as it avoids exposing secrets in  configuration files or environment variables.   <br><br>Example of configuration:   <br><br>```<br> polaris.remote-signing.key-files.key1=/etc/polaris/secrets/signing-key1<br> polaris.remote-signing.key-files.key2=/etc/polaris/secrets/signing-key2<br> ```<br><br> |
| `polaris.remote-sign.key-directory` |  | `path` | Path to a directory containing key files. Each file in the directory represents a signing key  where the filename is used as the key ID and the file content is the base64-encoded secret key.   <br><br>Each file should contain a single base64-encoded key that is exactly 32 bytes (256 bits)  when decoded. The file content is read at startup and trimmed of leading/trailing whitespace.   <br><br>This is particularly useful for Kubernetes deployments where secrets can be mounted as files  in a volume directory.   <br><br>Keys loaded from this directory will override keys with the same ID from (`#keys()`) or  (`#keyFiles()`).   <br><br>Example of configuration:   <br><br>```<br> polaris.remote-signing.key-directory=/etc/polaris/secrets/signing-keys/<br> ```<br><br>With files:   <br><br>```<br> /etc/polaris/secrets/signing-keys/key1  (content: base64 encoded key)<br> /etc/polaris/secrets/signing-keys/key2  (content: base64 encoded key)<br> ```<br><br> |
| `polaris.remote-sign.current-key-id` | `default` | `string` | The key ID to use for encrypting new tokens. <br><br>This key ID must exist in either the (`#keys()`) or (`#keyFiles()`) map. During key  rotation, update this value to the new key ID after adding the new key.  |
| `polaris.remote-sign.token-lifespan` | `PT7H` | `duration` | The duration for which signed parameters are valid. The default is 7 hours.   <br><br>After this duration, the signed parameters will be considered expired and the signing  request will be rejected.   <br><br>If this value is set to zero or negative, tokens will be created without an expiration time. |
| `polaris.remote-sign.encryption-method` | `A256CBC-HS512` | `string` | The JWE encryption method to use for encrypting tokens. The default is A256CBC-HS512.   <br><br>Supported encryption methods are:   <br><br> * `A128CBC-HS256` - AES-128-CBC with HMAC-SHA-256 (requires 32-byte key)    <br> * `A192CBC-HS384` - AES-192-CBC with HMAC-SHA-384 (requires 48-byte key)    <br> * `A256CBC-HS512` - AES-256-CBC with HMAC-SHA-512 (requires 64-byte key)    <br> * `A128GCM` - AES-128-GCM (requires 16-byte key)    <br> * `A192GCM` - AES-192-GCM (requires 24-byte key)    <br> * `A256GCM` - AES-256-GCM (requires 32-byte key)  <br><br>All configured keys must have the correct length for the selected encryption method. |
