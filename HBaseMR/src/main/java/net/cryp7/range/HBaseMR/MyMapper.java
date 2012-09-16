package net.cryp7.range.HBaseMR;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

public class MyMapper extends TableMapper<ImmutableBytesWritable, IntWritable>
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

/* EOF */

