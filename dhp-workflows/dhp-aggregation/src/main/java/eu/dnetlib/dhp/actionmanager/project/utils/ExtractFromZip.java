
package eu.dnetlib.dhp.actionmanager.project.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.PrepareProjects;
import eu.dnetlib.dhp.actionmanager.project.utils.model.Project;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;

/**
 * @author miriam.baglioni
 * @Date 28/02/23
 */
public class ExtractFromZip implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(PrepareProjects.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareProjects.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/project/extract_fromzip_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}: ", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		final String hdfsNameNode = parser.get("hdfsNameNode");
		log.info("hdfsNameNode {}", hdfsNameNode);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fs = FileSystem.get(conf);

		doExtract(inputPath, outputPath, fs);

	}

	private static void doExtract(String inputFile, String workingPath, FileSystem fileSystem)
		throws IOException {

		final Path path = new Path(inputFile);

		FSDataInputStream project_zip = fileSystem.open(path);

		try (ZipInputStream zis = new ZipInputStream(project_zip)) {
			ZipEntry entry = null;
			while ((entry = zis.getNextEntry()) != null) {

				if (!entry.isDirectory()) {
					String fileName = entry.getName();
					byte buffer[] = new byte[1024];
					int count;

					try (
						FSDataOutputStream out = fileSystem
							.create(new Path(workingPath + fileName))) {

						while ((count = zis.read(buffer, 0, buffer.length)) != -1)
							out.write(buffer, 0, count);

					}

				}

			}

		}

	}

}
