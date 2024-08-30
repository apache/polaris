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
title: 'Tabs'
type: docs
date: 2024-08-30T17:51:20+02:00
---

{{< tabpane text=true left=true >}}
  {{% tab header="**Languages**:" disabled=true /%}}
  {{% tab header="English" lang="en" %}}
  Welcome!
  {{% /tab %}}
  {{< tab header="German" lang="de" >}}
    <b>Herzlich willkommen!</b>
  {{< /tab >}}
  {{% tab header="Swahili" lang="sw" %}}
  **Karibu sana!**
  {{% /tab %}}
{{< /tabpane >}}

{{< tabpane left=true langEqualsHeader=true >}}
  {{% tab header="**Code in**:" disabled=true /%}}
  {{% tab header="Java" text=true %}}
Initial text

```java
String foo = "bar";
```

More text
  {{< /tab >}}
  {{% tab header="Python" text=true %}}
Some more markdown text...

```python
class ApiClient:
  PRIMITIVE_TYPES = (float, bool, bytes, str, int)
```

Hello World!
  {{% /tab %}}
  {{% tab header="Go" text=true %}}
Some code in Go

```go
foo = "bar";
```

More more
  {{% /tab %}}
{{< /tabpane >}}

{{< tabpane left=true langEqualsHeader=true >}}
  {{% tab header="**More code in**:" disabled=true /%}}
  {{< tab header="Java" >}}
    String bar = "baz";
  {{< /tab >}}
  {{< tab header="Python" >}}
    class ApiClient:
      PRIMITIVE_TYPES = (float, bool, bytes, str, int)
  {{< /tab >}}
  {{< tab header="Go" >}}
    bar = "baz";
  {{< /tab >}}
{{< /tabpane >}}
