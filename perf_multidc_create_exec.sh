#!/bin/bash
gradle clean fatJar
cd build/libs
echo "#! /usr/bin/env java -jar" > cam_perf_multidc-all
cat cam_perf_multidc-all.jar >> cam_perf_multidc-all
chmod +x cam_perf_multidc-all