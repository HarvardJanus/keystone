This is a temporary implementation of source extractor on KeystoneML. 
The hard-compiled libraries in lib is only for AWS instances.

#Download

cd /mnt

git clone https://github.com/zhaozhang/keystone.git

cd keystone

git checkout sourceextractor

sbt/sbt assembly

~/spark-ec2/copy-dir /mnt/keystone

#Configure Spark

edit spark-env.sh

    export SPARK_WORKER_INSTANCES=16
    
    export SPARK_WORKER_CORES=1
    
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/mnt/keystone/lib

edit spark-default.conf

    spark.executor.memory   4096m
    
    spark.locality.wait 0

#Run SourceExtractor

    KEYSTONE_MEM=4g bin/run-pipeline.sh pipelines.astronomy.SourceExtractor --inputPath /user/root/data