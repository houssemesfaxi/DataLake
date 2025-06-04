#!/bin/bash
export JAVA_HOME=/opt/bitnami/java
export PATH=$JAVA_HOME/bin:$PATH
echo "✅ JAVA_HOME = $JAVA_HOME"
python3 /scripts/minio_cv_scraper.py
