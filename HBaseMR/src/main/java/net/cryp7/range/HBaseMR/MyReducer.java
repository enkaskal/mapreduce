package net.cryp7.range.HBaseMR;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

public class MyReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>
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

/* EOF */



