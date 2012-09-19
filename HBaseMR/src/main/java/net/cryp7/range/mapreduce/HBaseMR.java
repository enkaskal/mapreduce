package net.cryp7.range.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

public class HBaseMR 
{
	private static final String appName = "net.cryp7.range.HBaseMR";
	
	public static class MyMapper extends TableMapper<ImmutableBytesWritable, IntWritable>
	{
		private int rows = 0;
	    private static final IntWritable one = new IntWritable(1);

	    @Override
	    public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException 
	    {
	        // extract userKey from the compositeKey (userId + counter)
	        ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);
	        
	        context.write(userKey, one);
	        
	        rows++;
	        if ((rows % 10000) == 0) 
	        {
	            context.setStatus("map processed " + rows + " rows so far");
	        }
	    }

	} /* end class MyMapper */
	
	public static class MyReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>
	{
		public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
	            throws IOException, InterruptedException 
	    {
	        int sum = 0;
	        for (IntWritable val : values) 
	        {
	            sum += val.get();
	        }

	        Put put = new Put(key.get());
	        put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
	        
	        System.out.println(String.format("stats :   key : %d,  count : %d", Bytes.toInt(key.get()), sum));
	        
	        context.write(key, put);
	    }

	} /* end class MyReducer */

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
                job,
                false
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
        
        System.out.println("done.");

	} /* end main */

} /* end class HBaseMR */

/* EOF */



