!#/bin/bash
./build/mvn -T 1C -pl :spark-sql_2.12,:spark-assembly_2.12 install  -DskipTests
