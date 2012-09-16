package net.cryp7.range.WordCountMR;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
	private Text word = new Text();
	private Text fname = new Text();
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output,
			Reporter reporter)
	throws IOException
	{
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);
		
		FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
		fname.set(fileSplit.getPath().getName());
		
		while ( itr.hasMoreTokens() )
		{
			word.set(itr.nextToken());
			output.collect(word, fname);
		}
		
	} /* end map */

} /* end class MyMapper */

/* EOF */

