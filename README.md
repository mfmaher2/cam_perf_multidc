# cam_perf_multidc

# Testing performance of multi data center replication

# Insert records in one DC and read it from another DC and register the latency between write and read

## Building

`gradle clean build`

## Building a fat/uber jar

`gradle clean fatJar`  This creates, cam_perf_multidc-all.jar in build/libs folder.

# Running tests

# arguments

`-dc1 - full path of data center 1 config file`
`-dc2 - full path of data center 2 config file`
`-i - how many read and write instances to query the database`

# explicitly call main program

`java -jar cam_perf_multidc-all.jar  com.fedex.cassandra.perf.MultiDCPerfTest -dc1 ~/config/dc1.conf -dc2 ~/config/dc2.conf -i 10`

# without the providing the main program

`java -jar cam_perf_multidc-all.jar -dc1 ~/config/dc1.conf -dc2 ~/config/dc2.conf -i 10`

# create an executable for running the test

`./perf_multidc_create_exec.sh`    -Creates an executable for the jar in build/libs

# run test using executable from above in build/libs

`./cam_perf_multidc-all -dc1 ~/config/dc1.conf -dc2 ~/config/dc2.conf -i 10`

# Note on local multi-dc testing

for local multi-dc testing a docker-compose file is provided in docker folder of the project. This creates two DC's with
one node in each DC.

start multi-dc docker, from the location where docker-compose file is located:
`docker compose -f docker-compose.yml up`

for checking if both DC's are up and running:
login into one of the container: `docker exec -it DSE-6_node1 bash`
run command at shell: `nodetool status`
