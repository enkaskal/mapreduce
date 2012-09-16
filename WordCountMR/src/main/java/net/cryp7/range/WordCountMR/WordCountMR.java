package net.cryp7.range.WordCountMR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountMR 
{
	private Job job;
	
	public WordCountMR(String inputPath, String outputPath) throws IOException
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop.range.cryp7.net:8020");
		conf.set("hbase.zookeeper.quorum", "hadoop.range.cryp7.net");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "hadoop.range.cryp7.net:60000");
		conf.set("mapred.job.tracker", "hadoop.range.cryp7.net:8021");
		
		job = new Job(conf);
        job.setJobName("WordCountMR");
        job.setJarByClass(MyMapper.class);
        
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
	} /* end c-tor WordCountMR(String inputPath, String outputPath) */
	
	public void run() throws IOException, InterruptedException, ClassNotFoundException
	{
		boolean success = job.waitForCompletion(true);
		if ( false == success )
		{
			System.err.println("job fuct");
			System.exit(1);
		}
	}

} /* end class WordCountMR */

/* EOF */

