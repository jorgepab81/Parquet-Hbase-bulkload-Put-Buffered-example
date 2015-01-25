package com.isbanuk.hadoop.hbase.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.example.data.Group;
import parquet.hadoop.example.ExampleInputFormat;

import org.apache.log4j.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class HbaseBulkLoadMap extends Configured implements Tool {
  //private static final Log LOG = LogFactory.getLog(TxnsBulkLoad.class);

  private static final String TABLE = "hbase.table";
  private static final String COLUMN_FAMILY = "hbase.cf";
  private static final String ROWKEY = "hbase.rowkey";
  private static final String COLUMN_QUALIFIERS = "hbase.cq";


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.out.println("Usage: <config> <input_path> <output_path>");
      System.out.println("<config>:");
      System.out.println("  -D hbase.table = <hbase_tablename>");
      System.out.println("  -D hbase.cf = <hbase_column_family>");
      System.out.println("  -D hbase.rowkey = <comma-separated list of source fields to use for rowkey>");
      System.out.println("  -D hbase.cq = <comma-separated list of source fields to use for columns>");
      GenericOptionsParser.printGenericCommandUsage(System.out);
      System.exit(1);
    }
    int res = ToolRunner.run(new Configuration(), new HbaseBulkLoadMap(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    /**Setting up 180 secs (2,5 min timeout instead of 600 s)
    /* Careful with this as it will wait this time for the Hbase commit. 
   	/* If the put batch takes longer than this for whatever reason, it will fail. **/
    conf.set("mapreduce.task.timeout", "1800000");  
    HBaseConfiguration.addHbaseResources(conf);
    
    Job job = new Job(conf, "Input Hbase table "+ conf.get(TABLE)+ " From path with parquet format "+args[0]);
    job.setJarByClass(getClass());
    
    job.setInputFormatClass(ExampleInputFormat.class);  //TextInputFormat by default for non text tables
    job.setMapperClass(ParquetMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);
    job.setNumReduceTasks(0);
    
    TableMapReduceUtil.addDependencyJars(job);
    //inserting the keys on the mapper. Setting up 0 reducers
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1])); //even not generating output, we do need this.
    System.out.println("######## Executing the job #####");

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  /**
   * Class for the mapper reading from a Parquet table and returning the KeyValue required for Hbase
   * @author Jorge Pablo
   *
   */
  private static class ParquetMapper extends Mapper<LongWritable, Group, ImmutableBytesWritable,KeyValue> {
    static final String DELIM = "::";
    private static final int NUM_BATCH = 10000;
    int numrecordsTried = 0;
    ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
    KeyValue kv;
    byte[] tableName;
    byte[] columnFamily;
    List<String> keys;
    List<String> columns;
    List<Put> puts = new ArrayList();
    StringBuilder rksb = new StringBuilder();
    int numputs=0;
	private HTable table = null;
    private static final Logger sLogger = Logger.getLogger(ParquetMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	
      Configuration conf = context.getConfiguration();
      //here the particular configuration needed for the mapper.
      //conf.set("hbase.zookeeper.quorum", "<quorum-url>");
      //conf.set("hbase.zookeeper.property.clientPort","2181");
      HBaseConfiguration.addHbaseResources(conf);
      Configuration HbaseConf =  HBaseConfiguration.create(conf);
	  table = new HTable(HbaseConf, conf.get(TABLE));
      tableName = conf.get(TABLE).getBytes();
      columnFamily = conf.get(COLUMN_FAMILY).getBytes();
      keys = Arrays.asList(conf.get(ROWKEY).split(","));
      columns = Arrays.asList(conf.get(COLUMN_QUALIFIERS).split(","));
    }

    @Override
    protected void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
    	//This mapper will return the values cleaned, only with the columns needed. The action will take part
    	//on the reduce part
       
	    List<KeyValue>  ListkeyV = getKeyValue(key,value,context);
	    numrecordsTried++;
	    Iterator it = ListkeyV.iterator();
	    while (it.hasNext()){
	    	Put p = new Put(rowKey.get());
	    	KeyValue keyv = (KeyValue) it.next();
	    	p.add(keyv);
	    	puts.add(p);  //adding it to a global object
	    	context.write(rowKey,keyv);
	    	context.getCounter("ParquetMapper", "RECORDS").increment(1);
	    }
	    // we load once we reach certain amount of puts on the list
	    if (puts.size()>NUM_BATCH)
    	  loadBulk();  									
    }
    
    
    @Override
    protected void cleanup(Context context
            ) throws IOException, InterruptedException {
    		table.close();
    		sLogger.info("####################################### Values read by this task: "+numrecordsTried);
    }
    
	private void loadBulk() {
		try {
			table.put(puts);
			numputs=0;
	  	  	puts.clear();
		} catch (RetriesExhaustedWithDetailsException e) {
			sLogger.error("Error retrying the connection for the put. Reason: "+e.getMessage());
		} catch (InterruptedIOException e) {
			sLogger.error("IO interrupted for the put. Reason: "+e.getMessage());
		} 
  	  	
		
	}

	private List<KeyValue> getKeyValue(LongWritable key, Group value, Mapper<LongWritable, Group, ImmutableBytesWritable, KeyValue>.Context context) {
		String line = value.toString();
    	List<String> fields = Arrays.asList(line.split("\n"));
    	List<KeyValue> listKv = new ArrayList();
	    rksb.setLength(0);
	    int rowkeyParts = 0;
	    boolean rowkeySet = false;
		 // obtaining the rowkey first, preserving order
	      for (String k : keys) {
	        for (String field : fields) {
	          String[] parts = field.split(": ");
	          if (parts.length != 2) continue;
	          if (k.trim().equals(parts[0].trim())) {
	            rksb.append(parts[1].trim() + DELIM);
	            rowkeyParts++;
	            break;
	          }
	        }
	      }

	      if (rksb.lastIndexOf(DELIM) > -1) rksb.delete(rksb.lastIndexOf(DELIM), rksb.length());
	      if (rowkeyParts == keys.size()) {
	        rowKey.set(rksb.toString().getBytes());
	        rowkeySet = true;
	      } else {
	        context.getCounter("ParquetMapper", "SKIPPED_INVALID_KEY").increment(1);
	      }

	      // Write columns
	      if (rowkeySet) {
	        for (String field : fields) {
	        	
	          String[] parts = field.split(": "); 
	          if (parts.length != 2) continue;
	          if (columns.contains(parts[0].trim())) {
	            kv = new KeyValue(rowKey.get(), columnFamily, parts[0].trim().getBytes(), parts[1].trim().getBytes());
	            //instead of loading here the table.put for the kv, we'll use 
	            //bulk upload. Hence returning a list so it can be loaded later on
	            listKv.add(kv);
	          }
	        }
	      }
		return listKv;
	}
  }
}