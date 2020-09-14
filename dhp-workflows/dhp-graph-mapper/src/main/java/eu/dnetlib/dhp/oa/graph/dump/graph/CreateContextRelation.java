
package eu.dnetlib.dhp.oa.graph.dump.graph;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.graph.*;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

/**
 * Writes the set of new Relation between the context and datasources. At the moment the relation between the context
 * and the project is not created because of a low coverage in the profiles of openaire ids related to projects
 */
public class CreateContextRelation implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(CreateContextEntities.class);
	private final Configuration conf;
	private final BufferedWriter writer;
	private final QueryInformationSystem queryInformationSystem;

	private static final String CONTEX_RELATION_DATASOURCE = "contentproviders";
	private static final String CONTEX_RELATION_PROJECT = "projects";

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				CreateContextRelation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_whole/input_entity_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String hdfsPath = parser.get("hdfsPath");
		log.info("hdfsPath: {}", hdfsPath);

		final String hdfsNameNode = parser.get("nameNode");
		log.info("nameNode: {}", hdfsNameNode);

		final String isLookUpUrl = parser.get("isLookUpUrl");
		log.info("isLookUpUrl: {}", isLookUpUrl);

		final CreateContextRelation cce = new CreateContextRelation(hdfsPath, hdfsNameNode, isLookUpUrl);

		log.info("Creating relation for datasource...");
		cce.execute(Process::getRelation, CONTEX_RELATION_DATASOURCE, ModelSupport.getIdPrefix(Datasource.class));

		log.info("Creating relations for projects... ");
//		cce
//			.execute(
//				Process::getRelation, CONTEX_RELATION_PROJECT,
//				ModelSupport.getIdPrefix(eu.dnetlib.dhp.schema.oaf.Project.class));

		cce.close();

	}

	private void close() throws IOException {
		writer.close();
	}

	public CreateContextRelation(String hdfsPath, String hdfsNameNode, String isLookUpUrl)
		throws IOException, ISLookUpException {
		this.conf = new Configuration();
		this.conf.set("fs.defaultFS", hdfsNameNode);

		queryInformationSystem = new QueryInformationSystem();
		queryInformationSystem.setIsLookUp(Utils.getIsLookUpService(isLookUpUrl));
		queryInformationSystem.execContextRelationQuery();

		FileSystem fileSystem = FileSystem.get(this.conf);
		Path hdfsWritePath = new Path(hdfsPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fsDataOutputStream = fileSystem.append(hdfsWritePath);
		} else {
			fsDataOutputStream = fileSystem.create(hdfsWritePath);
		}

		this.writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));

	}

	public void execute(final Function<ContextInfo, List<Relation>> producer, String category, String prefix) {

		final Consumer<ContextInfo> consumer = ci -> producer.apply(ci).forEach(c -> writeEntity(c));

		queryInformationSystem.getContextRelation(consumer, category, prefix);
	}

	protected void writeEntity(final Relation r) {
		try {
			writer.write(Utils.OBJECT_MAPPER.writeValueAsString(r));
			writer.newLine();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

}
