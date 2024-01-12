
package eu.dnetlib.dhp.collection.orcid;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ximpleware.NavException;
import com.ximpleware.ParseException;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

import eu.dnetlib.dhp.collection.orcid.model.Author;
import eu.dnetlib.dhp.collection.orcid.model.ORCIDItem;
import eu.dnetlib.dhp.parser.utility.VtdException;

public class DownloadORCIDTest {
	private final Logger log = LoggerFactory.getLogger(DownloadORCIDTest.class);

	@Test
	public void testSummary() throws Exception {
		final String xml = IOUtils
			.toString(
				Objects.requireNonNull(getClass().getResourceAsStream("/eu/dnetlib/dhp/collection/orcid/summary.xml")));

		final OrcidParser parser = new OrcidParser();
		ORCIDItem orcidItem = parser.parseSummary(xml);

		final ObjectMapper mapper = new ObjectMapper();
		System.out.println(mapper.writeValueAsString(orcidItem));

	}

	@Test
	public void testParsingWork() throws Exception {

		final List<String> works_path = Arrays
			.asList(
				"/eu/dnetlib/dhp/collection/orcid/activity_work_0000-0002-2536-4498.xml",
				"/eu/dnetlib/dhp/collection/orcid/activity_work_0000-0002-5982-8983.xml",
				"/eu/dnetlib/dhp/collection/orcid/activity_work_0000-0003-2760-1191.xml",
				"/eu/dnetlib/dhp/collection/orcid/activity_work_0000-0003-2760-1191-similarity.xml",
				"/eu/dnetlib/dhp/collection/orcid/activity_work_0000-0003-2760-1191_contributors.xml"

			);

		final OrcidParser parser = new OrcidParser();
		final ObjectMapper mapper = new ObjectMapper();
		works_path.stream().map(s -> {
			try {
				return IOUtils
					.toString(
						Objects
							.requireNonNull(
								getClass()
									.getResourceAsStream(
										s)));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}).forEach(s -> {
			try {
				System.out.println(mapper.writeValueAsString(parser.parseWork(s)));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Test
	public void testParsingEmployments() throws Exception {

		final List<String> works_path = Arrays
			.asList(
				"/eu/dnetlib/dhp/collection/orcid/employment.xml",
				"/eu/dnetlib/dhp/collection/orcid/employment_2.xml",
				"/eu/dnetlib/dhp/collection/orcid/employment_3.xml"

			);

		final OrcidParser parser = new OrcidParser();
		final ObjectMapper mapper = new ObjectMapper();
		works_path.stream().map(s -> {
			try {
				return IOUtils
					.toString(
						Objects
							.requireNonNull(
								getClass()
									.getResourceAsStream(
										s)));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}).forEach(s -> {
			try {
				System.out.println(mapper.writeValueAsString(parser.parseEmployment(s)));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

//	@Test
//	public void testReadTar() throws Exception {
//		OrcidGetUpdatesFile.main(new String[] {
//			"--namenode", "puppa"
//		});
//
//	}

}
