
package eu.dnetlib.dhp.oa.graph.dump.community;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.Provenance;
import eu.dnetlib.dhp.schema.dump.oaf.community.Funder;
import eu.dnetlib.dhp.schema.dump.oaf.community.Project;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

/**
 * Preparation of the Project information to be added to the dumped results. For each result associated to at least one
 * Project, a serialization of an instance af ResultProject class is done. ResultProject contains the resultId, and the
 * list of Projects (as in eu.dnetlib.dhp.schema.dump.oaf.community.Project) it is associated to
 */
public class SparkPrepareResultProject implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkPrepareResultProject.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkPrepareResultProject.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/project_prep_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				prepareResultProjectList(spark, inputPath, outputPath);
			});
	}

	private static void prepareResultProjectList(SparkSession spark, String inputPath, String outputPath) {
		Dataset<Relation> relation = Utils
			.readPath(spark, inputPath + "/relation", Relation.class)
			.filter("dataInfo.deletedbyinference = false and lower(relClass) = 'isproducedby'");
		Dataset<eu.dnetlib.dhp.schema.oaf.Project> projects = Utils
			.readPath(spark, inputPath + "/project", eu.dnetlib.dhp.schema.oaf.Project.class);

		projects
			.joinWith(relation, projects.col("id").equalTo(relation.col("target")), "inner")
			.groupByKey(
				(MapFunction<Tuple2<eu.dnetlib.dhp.schema.oaf.Project, Relation>, String>) value -> value
					._2()
					.getSource(),
				Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Tuple2<eu.dnetlib.dhp.schema.oaf.Project, Relation>, ResultProject>) (s,
					it) -> {
					Set<String> projectSet = new HashSet<>();
					Tuple2<eu.dnetlib.dhp.schema.oaf.Project, Relation> first = it.next();
					ResultProject rp = new ResultProject();
					rp.setResultId(s);
					eu.dnetlib.dhp.schema.oaf.Project p = first._1();
					projectSet.add(p.getId());
					Project ps = getProject(p);

					List<Project> projList = new ArrayList<>();
					projList.add(ps);
					rp.setProjectsList(projList);
					it.forEachRemaining(c -> {
						eu.dnetlib.dhp.schema.oaf.Project op = c._1();
						if (!projectSet.contains(op.getId())) {
							projList
								.add(getProject(op));

							projectSet.add(op.getId());

						}

					});
					return rp;
				}, Encoders.bean(ResultProject.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static Project getProject(eu.dnetlib.dhp.schema.oaf.Project op) {
		Project p = Project
			.newInstance(
				op.getId(),
				op.getCode().getValue(),
				Optional
					.ofNullable(op.getAcronym())
					.map(a -> a.getValue())
					.orElse(null),
				Optional
					.ofNullable(op.getTitle())
					.map(v -> v.getValue())
					.orElse(null),
				Optional
					.ofNullable(op.getFundingtree())
					.map(value -> {
						List<Funder> tmp = value
							.stream()
							.map(ft -> getFunder(ft.getValue()))
							.collect(Collectors.toList());
						if (tmp.size() > 0) {
							return tmp.get(0);
						} else {
							return null;
						}
					})
					.orElse(null));

		Optional<DataInfo> di = Optional.ofNullable(op.getDataInfo());
		Provenance provenance = new Provenance();
		if (di.isPresent()) {
			provenance.setProvenance(di.get().getProvenanceaction().getClassname());
			provenance.setTrust(di.get().getTrust());
			p.setProvenance(provenance);
		}

		return p;

	}

	private static Funder getFunder(String fundingtree) {
		// ["<fundingtree><funder><id>nsf_________::NSF</id><shortname>NSF</shortname><name>National Science
		// Foundation</name><jurisdiction>US</jurisdiction></funder><funding_level_1><id>nsf_________::NSF::CISE/OAD::CISE/CCF</id><description>Division
		// of Computing and Communication Foundations</description><name>Division of Computing and Communication
		// Foundations</name><parent><funding_level_0><id>nsf_________::NSF::CISE/OAD</id><description>Directorate for
		// Computer &amp; Information Science &amp; Engineering</description><name>Directorate for Computer &amp;
		// Information Science &amp;
		// Engineering</name><parent/><class>nsf:fundingStream</class></funding_level_0></parent></funding_level_1></fundingtree>"]
		Funder f = new Funder();
		final Document doc;
		try {
			doc = new SAXReader().read(new StringReader(fundingtree));
			f.setShortName(((Node) (doc.selectNodes("//funder/shortname").get(0))).getText());
			f.setName(((Node) (doc.selectNodes("//funder/name").get(0))).getText());
			f.setJurisdiction(((Node) (doc.selectNodes("//funder/jurisdiction").get(0))).getText());
			for (Object o : doc.selectNodes("//funding_level_0")) {
				List node = ((Node) o).selectNodes("./name");
				f.setFundingStream(((Node) node.get(0)).getText());

			}

			return f;
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		return f;
	}
}
