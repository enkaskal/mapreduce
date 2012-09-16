package net.cryp7.range.WordCountMR;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text>
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

/* EOF */

