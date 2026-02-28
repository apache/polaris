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
# This file creates the /downloads/latest/ redirect to the current latest stable download.
# Update the 'redirect_to' parameter below when publishing a new release.
title: 'Latest Download'
linkTitle: 'Latest'
type: 'redirect'
layout: 'redirect'
params:
  redirect_to: '/downloads/1.3.0/' # Replace after a Polaris release
hide_summary: true
exclude_search: true
toc_hide: true
menus:
  main:
    parent: downloads
    weight: -999998 # 2nd item in the menu
    identifier: downloads-latest
---
