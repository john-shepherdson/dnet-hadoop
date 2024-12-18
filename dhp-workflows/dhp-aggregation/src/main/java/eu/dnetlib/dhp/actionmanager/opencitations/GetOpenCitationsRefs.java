
package eu.dnetlib.dhp.actionmanager.opencitations;

import java.io.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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

//		final String[] inputFile = parser.get("inputFile").split(";");
//		log.info("inputFile {}", Arrays.asList(inputFile));

		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}", inputPath);

		final String hdfsNameNode = parser.get("hdfsNameNode");
		log.info("hdfsNameNode {}", hdfsNameNode);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}", outputPath);

		final String backupPath = parser.get("backupPath");
		log.info("backupPath {}", backupPath);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);

		GetOpenCitationsRefs ocr = new GetOpenCitationsRefs();

		ocr.doExtract(inputPath, outputPath, backupPath, fileSystem);

	}

	private void doExtract(String inputPath, String outputPath, String backupPath, FileSystem fileSystem)
		throws IOException {

		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem
			.listFiles(
				new Path(inputPath), true);
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			// do stuff with the file like ...
			FSDataInputStream oc_zip = fileSystem.open(fileStatus.getPath());
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
								.create(new Path(outputPath + "/" + fileName + ".gz"));
							GZIPOutputStream gzipOs = new GZIPOutputStream(new BufferedOutputStream(out))) {

							IOUtils.copy(zis, gzipOs);

						}
					}

				}

			}
			fileSystem.rename(fileStatus.getPath(), new Path(backupPath));
		}

	}

}
