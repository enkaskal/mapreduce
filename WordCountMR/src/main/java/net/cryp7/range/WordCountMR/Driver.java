package net.cryp7.range.WordCountMR;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;

public class Driver 
{
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException
	{
		if ( 2 != args.length )
		{
			System.err.println("Usage: net.cryp7.range.WordCountMR.Driver <hdfs://server[:port]/path/to/input/dir> <hdfs://server[:port]/path/to/output/dir/>");
			System.exit(1);
		}
		
		JobConf conf = new JobConf();
		conf.setJobName("wordcount");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
		System.out.println("fucking success!!!");
		
	} /* end main */

} /* end class Driver */

/* EOF */

