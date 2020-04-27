
package eu.dnetlib.dhp.actionmanager.migration;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class TransformActions implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(TransformActions.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String SEPARATOR = "/";

	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					MigrateActionSet.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/actionmanager/migration/transform_actionsets_parameters.json")));
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final String inputPaths = parser.get("inputPaths");

		if (StringUtils.isBlank(inputPaths)) {
			throw new RuntimeException("empty inputPaths");
		}
		log.info("inputPaths: {}", inputPaths);

		final String targetBaseDir = getTargetBaseDir(isLookupUrl);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf, isSparkSessionManaged, spark -> transformActions(inputPaths, targetBaseDir, spark));
	}

	private static void transformActions(String inputPaths, String targetBaseDir, SparkSession spark)
		throws IOException {
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		final FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

		for (String sourcePath : Lists.newArrayList(Splitter.on(",").split(inputPaths))) {

			LinkedList<String> pathQ = Lists.newLinkedList(Splitter.on(SEPARATOR).split(sourcePath));

			final String rawset = pathQ.pollLast();
			final String actionSetDirectory = pathQ.pollLast();

			final Path targetDirectory = new Path(targetBaseDir + SEPARATOR + actionSetDirectory + SEPARATOR + rawset);

			if (fs.exists(targetDirectory)) {
				log.info("found target directory '{}", targetDirectory);
				fs.delete(targetDirectory, true);
				log.info("deleted target directory '{}", targetDirectory);
			}

			log.info("transforming actions from '{}' to '{}'", sourcePath, targetDirectory);

			sc
				.sequenceFile(sourcePath, Text.class, Text.class)
				.map(a -> eu.dnetlib.actionmanager.actions.AtomicAction.fromJSON(a._2().toString()))
				.map(TransformActions::doTransform)
				.filter(Objects::nonNull)
				.mapToPair(
					a -> new Tuple2<>(a.getClazz().toString(), OBJECT_MAPPER.writeValueAsString(a)))
				.mapToPair(t -> new Tuple2(new Text(t._1()), new Text(t._2())))
				.saveAsNewAPIHadoopFile(
					targetDirectory.toString(),
					Text.class,
					Text.class,
					SequenceFileOutputFormat.class,
					sc.hadoopConfiguration());
		}
	}

	private static AtomicAction doTransform(eu.dnetlib.actionmanager.actions.AtomicAction aa)
		throws InvalidProtocolBufferException {

		// dedup similarity relations had empty target value, don't migrate them
		if (aa.getTargetValue().length == 0) {
			return null;
		}
		final OafProtos.Oaf proto_oaf = OafProtos.Oaf.parseFrom(aa.getTargetValue());
		final Oaf oaf = ProtoConverter.convert(proto_oaf);
		switch (proto_oaf.getKind()) {
			case entity:
				switch (proto_oaf.getEntity().getType()) {
					case datasource:
						return new AtomicAction<>(Datasource.class, (Datasource) oaf);
					case organization:
						return new AtomicAction<>(Organization.class, (Organization) oaf);
					case project:
						return new AtomicAction<>(Project.class, (Project) oaf);
					case result:
						final String resulttypeid = proto_oaf
							.getEntity()
							.getResult()
							.getMetadata()
							.getResulttype()
							.getClassid();
						switch (resulttypeid) {
							case "publication":
								return new AtomicAction<>(Publication.class, (Publication) oaf);
							case "software":
								return new AtomicAction<>(Software.class, (Software) oaf);
							case "other":
								return new AtomicAction<>(OtherResearchProduct.class, (OtherResearchProduct) oaf);
							case "dataset":
								return new AtomicAction<>(Dataset.class, (Dataset) oaf);
							default:
								// can be an update, where the resulttype is not specified
								return new AtomicAction<>(Result.class, (Result) oaf);
						}
					default:
						throw new IllegalArgumentException(
							"invalid entity type: " + proto_oaf.getEntity().getType());
				}
			case relation:
				return new AtomicAction<>(Relation.class, (Relation) oaf);
			default:
				throw new IllegalArgumentException("invalid kind: " + proto_oaf.getKind());
		}
	}

	private static String getTargetBaseDir(String isLookupUrl) throws ISLookUpException {
		ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);
		String XQUERY = "collection('/db/DRIVER/ServiceResources/ActionManagerServiceResourceType')//SERVICE_PROPERTIES/PROPERTY[@key = 'basePath']/@value/string()";
		return isLookUp.getResourceProfileByQuery(XQUERY);
	}
}
