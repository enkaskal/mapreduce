package net.cryp7.range.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountMR 
{
	private static final String appName = "net.cryp7.range.WordCountMR";
	
	@SuppressWarnings("unused")
	private static boolean IsDebuggerAttached()
	{
		return java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;
		
	} /* end IsDebuggerAttached() */
	
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
			System.err.println("Usage: " + appName + " hdfs://server[:port]/path/to/input/dir hdfs://server[:port]/path/to/output/dir");
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
		job.setJarByClass(WordCountMR.class);
        
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//DistributedCache.addArchiveToClassPath(new Path("hdfs://hadoop.range.cryp7.net:8020/user/stefan/WordCountMR-0.0.3-SNAPSHOT.jar"), conf);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		if ( false == success )
		{
			System.err.println("job failed");
			System.exit(1);
		}
		
		System.out.println("done.");
		
	} /* end main */
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		private static Text word = new Text();
		private static Text fname = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			fname.set(fileSplit.getPath().getName());
			
			while ( itr.hasMoreTokens() )
			{
				word.set(itr.nextToken());
				context.write(word, fname);
			}
			
		} /* end map */

	} /* end class MyMapper */
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String fnames = new String("");
			
			for ( Text val : values )
			{
				fnames += val.getBytes().toString() + ", ";
			}
			
			context.write(key, new Text(fnames));
			
		} /* end reduce */

	} /* end class MyReducer */
	
} /* end class WordCountMR */

/* EOF */

