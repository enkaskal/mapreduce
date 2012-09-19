package net.cryp7.range.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConfigFile 
{
	private Properties properties;
	
	private void setDefaultProperties()
	{
		properties.setProperty("jar", "PleaseSpecifyAJarFile.jar");
		
	} /* end setDefaultProperties */
	
	public ConfigFile(String[] args) throws IOException
	{
		properties = new Properties();
		setDefaultProperties();
		
		/* see if user attempted to overwrite on the CLI as a system property */
		for ( String key : properties.stringPropertyNames() )
		{
			String value = System.getProperty(key);
			if ( null != value )
			{
				properties.setProperty(key, value);
			}
			
		} /* end for ( properties ) */
		
		/* allow user to also specify properties in a file */
		FileInputStream propertiesFile = null;
		try
		{
			propertiesFile = new FileInputStream(args[0]);
			properties.load(propertiesFile);
		}
		catch ( Exception e )
		{
			System.out.println("loading properties from " + args[0] + "failed: " + e.getLocalizedMessage());
		}
		finally
		{
			if ( null != propertiesFile )
			{
				propertiesFile.close();
			}
			
		} /* end try to parse properties from file */
		
	} /* end c-tor ConfigFile(String[] args) */
	
	public String getValue(String key)
	{
		return properties.getProperty(key);
		
	} /* end getValue() */
	public SortedMap<String,String> getSubKeyValues(String subKey)
	{
		/* get all the keys */
		Set<String> keys = properties.stringPropertyNames();
		
		SortedMap<String,String> rv = new TreeMap<String,String>();
		for ( String key : keys )
		{
			/* check if this key is one of our subkeys */ 
			if ( key.startsWith(subKey) )
			{
				rv.put(key, getValue(key));
				
			} /* end if we found a key that matches our subkey */
			
		} /* end for all keys in properties */
		
		return rv;
		
	} /* end getSubKeyValues */
	
	public String[] getSubValues(String subKey)
	{
		Collection<String> strings = getSubKeyValues(subKey).values();
		String[] rv = new String[strings.size()];
		strings.toArray(rv);
		return rv;
		
	} /* get SubValues() */
	
	public String[] getAllValues()
	{
		return getSubValues("");
		
	} /* end getAllValues() */
	
} /* end class getSubValues */

/* EOF */

