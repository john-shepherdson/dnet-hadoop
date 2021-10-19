
package eu.dnetlib.dhp.oa.graph.dump.complete;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.graph.ResearchInitiative;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

/**
 * Writes on HDFS Context entities. It queries the Information System at the lookup url provided as parameter and
 * collects the general information for contexes of type community or ri. The general information is the id of the
 * context, its label, the subjects associated to the context, its zenodo community, description and type. This
 * information is used to create a new Context Entity
 */
public class CreateContextEntities implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(CreateContextEntities.class);
	private final transient Configuration conf;
	private final transient BufferedWriter writer;

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				CreateContextEntities.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/complete/input_entity_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final String hdfsPath = parser.get("hdfsPath");
		log.info("hdfsPath: {}", hdfsPath);

		final String hdfsNameNode = parser.get("nameNode");
		log.info("nameNode: {}", hdfsNameNode);

		final String isLookUpUrl = parser.get("isLookUpUrl");
		log.info("isLookUpUrl: {}", isLookUpUrl);

		final CreateContextEntities cce = new CreateContextEntities(hdfsPath, hdfsNameNode);

		log.info("Processing contexts...");
		cce.execute(Process::getEntity, isLookUpUrl);

		cce.close();

	}

	private void close() throws IOException {
		writer.close();
	}

	public CreateContextEntities(String hdfsPath, String hdfsNameNode) throws IOException {
		this.conf = new Configuration();
		this.conf.set("fs.defaultFS", hdfsNameNode);
		FileSystem fileSystem = FileSystem.get(this.conf);
		Path hdfsWritePath = new Path(hdfsPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fsDataOutputStream = fileSystem.append(hdfsWritePath);
		} else {
			fsDataOutputStream = fileSystem.create(hdfsWritePath);
		}
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodecByClassName("org.apache.hadoop.io.compress.GzipCodec");

		this.writer = new BufferedWriter(new OutputStreamWriter(codec.createOutputStream(fsDataOutputStream),
			StandardCharsets.UTF_8));

	}

	public <R extends ResearchInitiative> void execute(final Function<ContextInfo, R> producer, String isLookUpUrl)
		throws ISLookUpException {

		QueryInformationSystem queryInformationSystem = new QueryInformationSystem();
		queryInformationSystem.setIsLookUp(Utils.getIsLookUpService(isLookUpUrl));

		final Consumer<ContextInfo> consumer = ci -> writeEntity(producer.apply(ci));

		queryInformationSystem.getContextInformation(consumer);
	}

	protected <R extends ResearchInitiative> void writeEntity(final R r) {
		try {
			writer.write(Utils.OBJECT_MAPPER.writeValueAsString(r));
			writer.newLine();
		} catch (final IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

}
