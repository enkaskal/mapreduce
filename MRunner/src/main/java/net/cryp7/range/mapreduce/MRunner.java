package net.cryp7.range.mapreduce;

import org.apache.hadoop.util.RunJar;

public class MRunner 
{
	private static final String appName = "net.range.cryp7.MRunner";

	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		/* tell user how to run; if need be */
		if ( 1 != args.length )
		{
			System.err.println("Usage: " + appName + " /path/to/config/file");
			System.exit(1);
		}

		/* read the config file */
		ConfigFile cf = null;
		try
		{
			cf = new ConfigFile(args);
		}
		catch ( Exception e )
		{
			System.err.println("unable to parse config file: " + args[0]);
			System.exit(1);
		}

		/* get the jar name */
		String jar = cf.getValue("jar");
		
		/* get all the jar arguments */
		String[] jarArgs = cf.getSubValues("args");
		String[] hadoopArgs = new String[1 + jarArgs.length];
		
		/* construct the appropriate "jar args" strings to pass
		 * 
		 * N.B. effectively we're creating a String[] to pass to CLI: 
		 * hadoop jar ${hadoopArgs} where ${hadoopArgs} = "jar arg0 arg1 ..."
		 */
		hadoopArgs[0] = jar;
		for ( int i = 1; i < hadoopArgs.length; i++ )
		{
			hadoopArgs[i] = jarArgs[i - 1];
		}
		
		/* run the job in the cluster */
		try
		{
			/* run the jar */
			RunJar.main(hadoopArgs);
		}
		catch ( Throwable e )
		{
			System.err.println("unable to task cluster: " + e.getLocalizedMessage());
			System.exit(1);
		}
		
		/* declare success :D */
		System.out.println("done.");

	} /* end main */
	
} /* end class MRunner */

/* EOF */

