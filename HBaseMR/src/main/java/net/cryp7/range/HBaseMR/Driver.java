package net.cryp7.range.HBaseMR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

public class Driver 
{

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://hadoop.range.cryp7.net:8020");
		conf.set("hbase.zookeeper.quorum", "hadoop.range.cryp7.net");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "hadoop.range.cryp7.net:60000");
		conf.set("mapred.job.tracker", "hadoop.range.cryp7.net:8021");
		
        Job job = new Job(conf);
        job.setJobName("HBaseMR");
        job.setJarByClass(MyMapper.class);
        
        Scan scan = new Scan();
        scan.setCacheBlocks(true);
        scan.setCaching(5000);
        
        TableMapReduceUtil.initTableMapperJob(
        		"access_logs", 
        		scan, 
        		MyMapper.class, 
        		ImmutableBytesWritable.class,
                IntWritable.class, 
                job
        );
        
        TableMapReduceUtil.initTableReducerJob(
        		"summary_user",
        		MyReducer.class, 
        		job
        );
        
        if ( false == job.waitForCompletion(true) )
        {
        	System.out.println("job fuct");
        }

	} /* end main */

} /* end class Driver */

/* EOF */



