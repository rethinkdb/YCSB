<!--
Copyright (c) 2013 - 2018 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on RethinkDB in a Linux Box. Some parts are 
specific to **_Ubuntu Xenial_** but can be adapted for other distributions. 

### 1. Install and start RethinkDB
#### a. Using Docker
Ensure you have the latest version of docker 
[installed](https://docs.docker.com/install/linux/docker-ce/ubuntu/).
 
On the your terminal, run:
```bash
docker run --name rethinkdb-bench -p "8080:8080" -p "28015:28015" -d rethinkdb
```
This will pull the `rethinkdb` docker image with the `latest` tag if it is not
available locally. It will then start a container called `rethinkdb-bench` as a
daemon and map ports `8080` and `28015` on the host machine to the container's 
ports `8080` and `28015`, respectively.

### b. Using distribution specific packages
Follow [these instructions](https://rethinkdb.com/docs/install/ubuntu/).

### 2. Install Java and Maven 3

Install java by following these
 [instructions](https://www.digitalocean.com/community/tutorials/how-to-install-java-with-apt-get-on-ubuntu-16-04)
 for instance.

Then install maven by following the
 [official installation instructions](https://maven.apache.org/install.html) or 
 [these instructions](https://www.vultr.com/docs/how-to-install-apache-maven-on-ubuntu-16-04)
 specific to Ubuntu Xenial.

### 3. Set Up YCSB

Clone this repository:
```bash
git clone https://github.com/rethinkdb/YCSB.git
```
Then, to build the full distribution, with all database bindings:
```bash
mvn clean package
```
To build a single database binding:
```$xslt
mvn -pl com.yahoo.ycsb:rethinkdb-binding -am clean package
```

### 4. Run YCSB

Now you are ready to run! First, load the data:

    ./bin/ycsb load rethinkdb -s -P workloads/workloada > outputLoad.txt

Then, run the workload:

    ./bin/ycsb run rethinkdb -s -P workloads/workloada > outputRun.txt

## RethinkDB Configuration Parameters
The database is called `ycsb` by default. But the following can be configured as desired:
- `rethinkdb.host`
  - The default is `localhost`.
- `rethinkdb.port`
  - The default is `28015`. This is the port at which rethinkdb listens for client driver connections.
- `rethinkdb.durability`
  - The default is `hard`.
  - Allowed values are:
    - `hard`
    - `soft`
- `rethinkdb.table`
  - The default is `usertable`.
- `rethinkdb.readmode`
  - The default is `single`.
  - Allowed values are:
    - `single`
    - `majority`
    - `outdated`

See the [docs on consistency](https://rethinkdb.com/docs/consistency/)
 to understand `durability` and `readmode`.

To pass this parameters, use the `-p` flag:
```bash
./bin/ycsb load rethinkdb -s -P workloads/workloada -p rethinkdb.host=localhost
```

For more information on how to run workloads, see the
 [YCSB Wiki: Running a Workload](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload).