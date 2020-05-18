
package eu.dnetlib.dhp.actionmanager.project;

import java.io.*;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class GetFile {

	private static final Log log = LogFactory.getLog(GetFile.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GetFile.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/actionmanager/project/parameters.json")));

		Configuration conf = new Configuration();

		parser.parseArgument(args);

		final String fileURL = parser.get("fileURL");
		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("hdfsNameNode");

		conf.set("fs.defaultFS", hdfsNameNode);
		FileSystem fileSystem = FileSystem.get(conf);
		Path hdfsWritePath = new Path(hdfsPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fsDataOutputStream = fileSystem.append(hdfsWritePath);
		} else {
			fsDataOutputStream = fileSystem.create(hdfsWritePath);
		}

		InputStream is = new BufferedInputStream(new URL(fileURL).openStream());

		org.apache.hadoop.io.IOUtils.copyBytes(is, fsDataOutputStream, 4096, true);

	}

}
