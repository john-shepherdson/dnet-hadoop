
package eu.dnetlib.dhp.application;

import java.io.*;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

public class ApplicationUtils {

	public static Configuration getHadoopConfiguration(String nameNode) {
		// ====== Init HDFS File System Object
		Configuration conf = new Configuration();
		// Set FileSystem URI
		conf.set("fs.defaultFS", nameNode);
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		System.setProperty("hadoop.home.dir", "/");
		return conf;
	}

	public static void populateOOZIEEnv(final Map<String, String> report) throws IOException {
		File file = new File(System.getProperty("oozie.action.output.properties"));
		Properties props = new Properties();
		report.forEach((k, v) -> props.setProperty(k, v));

		try(OutputStream os = new FileOutputStream(file)) {
			props.store(os, "");
		}
	}

	public static void populateOOZIEEnv(final String paramName, String value) throws IOException {
		Map<String, String> report = Maps.newHashMap();
		report.put(paramName, value);

		populateOOZIEEnv(report);
	}

}
