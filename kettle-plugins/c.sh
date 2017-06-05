#!/bin/sh
set -x

#rm -rf /data/ee/0517/data-integration/system/karaf/caches
#mkdir -p /data/ee/0517/data-integration/system/karaf/system/pentaho/pentaho-big-data-kettle-plugins-parquet/8.0-SNAPSHOT/
#cp parquet/target/pentaho-big-data-kettle-plugins-parquet-8.0-SNAPSHOT.jar /data/ee/0517/data-integration/system/karaf/system/pentaho/pentaho-big-data-kettle-plugins-parquet/8.0-SNAPSHOT/

cd parquet
mvn clean install || exit 1
cd ..
cp parquet/target/pentaho-big-data-kettle-plugins-parquet-8.0-SNAPSHOT.jar /data/ee/0517/data-integration/plugins/pq/
