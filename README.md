Ingestion of HBase records from a Hdfs file via Put using the WAL and using buffering

Package that loads a hdfs file with PARQUET format into Hbase via WAL (using Put). This package is based on using only the Mapper to load the table. Shortly I'll add how to do it with the reducer plus another version using the MR driver to flush all the puts automatically without the needs of buffering control Using the TAblemapper class. Another example: https://github.com/jrkinley/hbase-bulk-import-example/blob/master/src/main/java/com/cloudera/examples/hbase/bulkimport/HBaseKVMapper.java

In this example added, the performance improved cleary adding more buffered items from 1 to 1000, reducing the time from 25mins to 50s based on a cluster of 14 nodes. However, increasing the buffering to 10K, the response wasn't as good as the timeout comes to play.

The process is simple. Fist the job is configured adding all the Hbase needed resources. Note that the addDependencyJars sometimes is needed so the nodes where the mapper runs takes the jars needed to deal with Hbase.

In the mapper, the connection is configured in the SetUP to avoid n-executions setting up the mapper on each iteration. Also note that there are two conf.set commented. Sometimes the nodes need some more info if the hadoop system is not 100% configured. Note that the 'main' and 'run' are running normally in the Gateway and the mapper in the nodes, so the configuration doesn't need to be the same.

The mapper then obtains the KV to insert in the Hbase table and enqueue it in an object 'List' that will keep them till it reaches the threshold marked. Then it will load them at once into Hbase, so the multiple connections is avoided.

Just to finish, the table is closed in the method clear().

Note also that even if the output is not generated, we still need to tell the MR job what output file we are using.

To launch the package:

$ hadoop jar hbase-bulkload-audit-txns-1.0.0.jar com.isbanuk.hadoop.hbase.mr.TxnsBulkLoadMap \
      -D hbase.bulkload.table=#table-name# \
      -D hbase.bulkload.cf=#family-column# \
      -D hbase.bulkload.rowkey=#Rowkey-Hbase# \
      -D hbase.bulkload.cq=#list-qualifiers# \
      #Hdfs-Input-File# \
      #Hdfs-output-file# \

Thanks a lot to JRkingley for his support in the extension of this MR! (https://github.com/jrkinley)

