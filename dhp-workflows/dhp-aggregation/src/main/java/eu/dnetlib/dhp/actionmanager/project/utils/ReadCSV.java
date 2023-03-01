
package eu.dnetlib.dhp.actionmanager.project.utils;

import java.io.*;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.GetCSV;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

/**
 * Applies the parsing of a csv file and writes the Serialization of it in hdfs
 */
public class ReadCSV {

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					ReadCSV.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/actionmanager/project/parameters.json")));

		parser.parseArgument(args);

		final String fileURL = parser.get("fileURL");
		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("hdfsNameNode");
		final String classForName = parser.get("classForName");
		Optional<String> delimiter = Optional.ofNullable(parser.get("delimiter"));
		char del = ';';
		if (delimiter.isPresent())
			del = delimiter.get().charAt(0);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);

		FSDataInputStream inputStream = fileSystem.open(new Path(fileURL));

		BufferedReader reader = new BufferedReader(
			new InputStreamReader(inputStream));

		GetCSV.getCsv(fileSystem, reader, hdfsPath, classForName, del);

		reader.close();

	}

}
