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

public class Main 
{
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{
		if ( 2 != args.length )
		{
			System.err.println("Usage: WordCountMR hdfs://server[:port]/path/to/input/dir hdfs://server[:port]/path/to/output/dir");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop.range.cryp7.net:8020");
		conf.set("hbase.zookeeper.quorum", "hadoop.range.cryp7.net");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "hadoop.range.cryp7.net:60000");
		conf.set("mapred.job.tracker", "hadoop.range.cryp7.net:8021");
		
		Job job = new Job(conf);
        job.setJobName("WordCountMR");
        job.setJarByClass(MyMapper.class);
        
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		if ( false == success )
		{
			System.err.println("job fuct");
			System.exit(1);
		}
		
	} /* end main */

} /* end class WordCountMR */

/* EOF */

