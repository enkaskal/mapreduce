package net.cryp7.range.HBaseMR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
		
        Job job = new Job(conf, "HBaseMR");
        job.setJarByClass(MyMapper.class);
        
        Scan scan = new Scan();
        scan.setCacheBlocks(true);
        scan.setCaching(5000);
        
        HBaseAdmin admin = new HBaseAdmin(conf);
        String outputTable = "summary_user";
        if ( admin.tableExists(outputTable) )
        {
        	/* if exists drop first */
        	admin.disableTable(outputTable);
        	admin.deleteTable(outputTable);
        }
        
        HTableDescriptor descriptor = new HTableDescriptor(outputTable);
        descriptor.addFamily(new HColumnDescriptor("details"));
        admin.createTable(descriptor);
        
        admin.close();
        
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



