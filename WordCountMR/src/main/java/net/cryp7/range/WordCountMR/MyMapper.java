package net.cryp7.range.WordCountMR;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private Text word = new Text();
	private Text fname = new Text();
	
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

/* EOF */

