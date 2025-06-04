#!/bin/bash
mkdir -p /opt/java
cd /opt/java
curl -L -o openjdk11.tar.gz https://download.bell-sw.com/java/11.0.19+7/bellsoft-jdk11.0.19+7-linux-amd64.tar.gz
tar -xzf openjdk11.tar.gz
mv bellsoft-jdk11.0.19+7 jdk-11
export JAVA_HOME=/opt/java/jdk-11
export PATH=$JAVA_HOME/bin:$PATH
echo "✅ JAVA_HOME = $JAVA_HOME"
java -version
python3 /scripts/minio_cv_scraper.py
