package net.cryp7.range.mapreduce;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Ingest 
{

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{
		String[] pages = { "/", "/a.html", "/b.html", "/c.html" };

		@SuppressWarnings("deprecation")
		HBaseConfiguration hbaseConfig = new HBaseConfiguration();
		HTable htable = new HTable(hbaseConfig, "access_logs");
		htable.setAutoFlush(false);
		htable.setWriteBufferSize(1024 * 1024 * 12);

		int totalRecords = 100000;
		int maxID = totalRecords / 1000;
		Random rand = new Random();
		System.out.println("importing " + totalRecords + " records ....");
		for (int i = 0; i < totalRecords; i++) {
			int userID = rand.nextInt(maxID) + 1;
			byte[] rowkey = Bytes.add(Bytes.toBytes(userID), Bytes.toBytes(i));
			String randomPage = pages[rand.nextInt(pages.length)];
			Put put = new Put(rowkey);
			put.add(Bytes.toBytes("details"), Bytes.toBytes("page"),
					Bytes.toBytes(randomPage));
			htable.put(put);
		}
		htable.flushCommits();
		htable.close();
		System.out.println("done");
		
	} /* end main */

} /* end class ingest */

/* EOF */

