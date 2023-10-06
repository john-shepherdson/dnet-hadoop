
package eu.dnetlib.dhp.actionmanager.opencitations;

import java.io.*;
import java.io.Serializable;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class GetOpenCitationsRefs implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(GetOpenCitationsRefs.class);

	public static void main(final String[] args) throws IOException, ParseException {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							GetOpenCitationsRefs.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/actionmanager/opencitations/input_parameters.json"))));

		parser.parseArgument(args);

		final String[] inputFile = parser.get("inputFile").split(";");
		log.info("inputFile {}", inputFile.toString());

		final String workingPath = parser.get("workingPath");
		log.info("workingPath {}", workingPath);

		final String hdfsNameNode = parser.get("hdfsNameNode");
		log.info("hdfsNameNode {}", hdfsNameNode);

		final String prefix = parser.get("prefix");
		log.info("prefix {}", prefix);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);

		GetOpenCitationsRefs ocr = new GetOpenCitationsRefs();

		for (String file : inputFile) {
			ocr.doExtract(workingPath + "/Original/" + file, workingPath, fileSystem, prefix);
		}

	}

	private void doExtract(String inputFile, String workingPath, FileSystem fileSystem, String prefix)
		throws IOException {

		final Path path = new Path(inputFile);

		FSDataInputStream oc_zip = fileSystem.open(path);

		// int count = 1;
		try (ZipInputStream zis = new ZipInputStream(oc_zip)) {
			ZipEntry entry = null;
			while ((entry = zis.getNextEntry()) != null) {

				if (!entry.isDirectory()) {
					String fileName = entry.getName();
					// fileName = fileName.substring(0, fileName.indexOf("T")) + "_" + count;
					fileName = fileName.substring(0, fileName.lastIndexOf("."));
					// count++;
					try (
						FSDataOutputStream out = fileSystem
							.create(new Path(workingPath + "/" + prefix + "/" + fileName + ".gz"));
						GZIPOutputStream gzipOs = new GZIPOutputStream(new BufferedOutputStream(out))) {

						IOUtils.copy(zis, gzipOs);

					}
				}

			}

		}

	}

}
