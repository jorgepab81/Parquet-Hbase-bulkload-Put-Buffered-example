/**
 * To run the bulk load tool:
 * <pre>
 * {@code

 *   # hadoop jar hbase-bulkload-audit-txns-1.0.0.jar com.isbanuk.hadoop.hbase.mr.TxnsBulkLoad \
 *     -D hbase.bulkload.table=<table-name> \
 *     -D hbase.bulkload.cf=<family-column> \
 *     -D hbase.bulkload.rowkey=<Rowkey-Hbase> \
 *     -D hbase.bulkload.cq=<list-qualifiers> \
 *     <Hdfs-Input-File> \
 *     <Hdfs-output-file> \
 *   #
 * }
 * </pre>
 */
/**
 * @author Jorge Pablo
 *
 */
package com.isbanuk.hadoop.hbase.mr;