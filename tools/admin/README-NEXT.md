<!--
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

## Administrative Operations

The `wskadmin-next` utility is handy for performing various administrative operations against an OpenWhisk deployment.
It allows you to create a new subject, manage their namespaces, to block a subject or delete their record entirely.

This is a scala based implementation of `wskadmin` utility and is meant to be DB agnostic.

### Build

To build the tool run

    $./gradlew :tools:admin:build

This creates a jar at `tools/admin/build/libs/openwhisk-admin-tools-1.0.0-SNAPSHOT-cli.jar` and install it as an executable script at
`bin/wskadmin-next`.

### Setup

Build task creates an executable at `bin/wskadmin-next`. By default, the config related to `ArtifactStore` for accessing database will read the `$OPENWHISK_HOME/whisk.conf`, which was generated by ansible `properties` deployment. Alternatively, the required config can be also passed by an overwritten config file. For example to access user details from default CouchDB setup create a file `application-cli.conf`.

    include classpath("application.conf")

    whisk {
      couchdb {
        protocol = "http"
        host     = "172.17.0.1"
        port     = "5984"
        username = "whisk_admin"
        password = "some_passw0rd"
        provider = "CouchDB"
        databases {
          WhiskAuth       = "whisk_local_subjects"
          WhiskEntity     = "whisk_local_whisks"
          WhiskActivation = "whisk_local_activations"
        }
      }
    }

And pass that to command via `-c` option.

    $./wskadmin-next -c application-cli.conf user get guest


### Managing Users (subjects)

The `wskadmin-next user -h` command prints the help message for working with subject records. You can create and delete a
new user, list all their namespaces or keys for a specific namespace, identify a user by their key, block/unblock a subject,
and list all keys that have access to a particular namespace.

Some examples:
```bash
# create a new user
$ wskadmin-next user create userA
<prints key>

# add user to a specific namespace
$ wskadmin-next user create userA -ns space1
<prints new key specific to userA and space1>

# add second user to same space
$ wskadmin-next user create userB -ns space1
<prints new key specific to userB and space1>

# list all users sharing a space
$ wskadmin-next user list space1 -a
<key for userA>   userA
<key for userB>   userB

# remove user access to a namespace
$ wskadmin-next user delete userB -ns space1
Namespace deleted

# get key for userA default namespaces
$ wskadmin-next user get userA
<prints key specific to userA default namespace>

# block a user
$ wskadmin-next user block userA
"userA" blocked successfully

# unblock a user
$ wskadmin-next user unblock userA
"userA" unblocked successfully

# delete user
$ wskadmin-next user delete userB
Subject deleted
```

The `wskadmin-next limits` commands allow you set action and trigger throttles per namespace.

```bash
# see if custom limits are set for a namespace
$ wskadmin-next limits get space1
No limits found, default system limits apply

# set limits
$ wskadmin-next limits set space1 --invocationsPerMinute 1
Limits successfully set for "space1"
```

Note that limits apply to a namespace and will survive even if all users that share a namespace are deleted. You must manually delete them.
```bash
$ wskadmin-next limits delete space1
Limits deleted
```