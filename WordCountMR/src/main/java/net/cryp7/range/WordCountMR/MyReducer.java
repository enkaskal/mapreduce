package net.cryp7.range.WordCountMR;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MyReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
	@Override
	public void reduce( Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output,
			Reporter reporter)
	throws IOException
	{
		String fnames = new String("");
		
		while ( values.hasNext() )
		{
			fnames += values.next().toString() + ", ";
		}
		
		output.collect(key,  new Text(fnames));
		
	} /* end reduce */

} /* end class MyReducer */

/* EOF */

