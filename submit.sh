#! /bin/bash

spark-submit \
  --class  sparky.com.samklr.adm.Analytics \
  --master spark://spark:7077 \
  --deploy-mode client \
  /home/samklr/parquetter/target/scala-2.10/Parquetter-assembly-0.1.0-SNAPSHOT.jar
