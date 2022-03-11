
package eu.dnetlib.dhp.oa.graph.hostedbymap;

import static eu.dnetlib.dhp.common.collection.DecompressTarGz.doExtract;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.DOAJModel;
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj.DOAJEntry;

public class ExtractAndMapDoajJson {

	private static final Logger log = LoggerFactory.getLogger(ExtractAndMapDoajJson.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							ExtractAndMapDoajJson.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/oa/graph/hostedbymap/download_json_parameters.json"))));

		parser.parseArgument(args);

		final String compressedInput = parser.get("compressedFile");
		log.info("compressedInput {}", compressedInput);

		final String hdfsNameNode = parser.get("hdfsNameNode");
		log.info("hdfsNameNode {}", hdfsNameNode);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}", outputPath);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath {}", workingPath);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fs = FileSystem.get(conf);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodecByClassName("org.apache.hadoop.io.compress.GzipCodec");
		doExtract(fs, workingPath, compressedInput);
		doMap(fs, workingPath, outputPath, codec);

	}

	private static void doMap(FileSystem fs, String workingPath, String outputPath, CompressionCodec codec)
		throws IOException {
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs
			.listFiles(
				new Path(workingPath), true);

		Path hdfsWritePath = new Path(outputPath);
		if (fs.exists(hdfsWritePath)) {
			fs.delete(hdfsWritePath, true);

		}
		try (

			FSDataOutputStream out = fs
				.create(hdfsWritePath);
			PrintWriter writer = new PrintWriter(new BufferedOutputStream(out))) {

			while (fileStatusListIterator.hasNext()) {
				Path path = fileStatusListIterator.next().getPath();
				if (!fs.isDirectory(path)) {
					FSDataInputStream is = fs.open(path);
					CompressionInputStream compressionInputStream = codec.createInputStream(is);
					DOAJEntry[] doajEntries = new ObjectMapper().readValue(compressionInputStream, DOAJEntry[].class);
					Arrays.stream(doajEntries).forEach(doaj -> {
						try {
							writer.println(new ObjectMapper().writeValueAsString(getDoajModel(doaj)));
						} catch (JsonProcessingException e) {
							e.printStackTrace();
						}
					});
				}

			}

		}

	}

	@NotNull
	public static DOAJModel getDoajModel(DOAJEntry doaj) {
		DOAJModel doajModel = new DOAJModel();
		doajModel.setOaStart(doaj.getBibjson().getOa_start());
		doajModel.setEissn(doaj.getBibjson().getEissn());
		doajModel.setIssn(doaj.getBibjson().getPissn());
		doajModel.setJournalTitle(doaj.getBibjson().getTitle());
		doajModel.setReviewProcess(doaj.getBibjson().getEditorial().getReview_process());
		return doajModel;
	}

}
