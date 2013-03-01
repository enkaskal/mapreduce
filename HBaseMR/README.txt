from: http://sujee.net/tech/articles/hadoop/hbase-map-reduce-freq-counter/

1. need to create the tables manually using hbase shell

hbase shell
    create 'access_logs', 'details'
    create 'summary_user', {NAME=>'details', VERSIONS=>1}

2. ingest data

java -cp HBaseMR-0.0.3-SNAPSHOT-jar-with-dependencies.jar net.cryp7.range.mapreduce.Ingest

3. mapreduce

hadoop jar HBaseMR-0.0.3-SNAPSHOT-jar-with-dependencies.jar net.cryp7.range.mapreduce.HBaseMR


