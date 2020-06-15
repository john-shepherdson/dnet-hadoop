
package eu.dnetlib.dhp.oa.graph.dump;

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
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.dump.oaf.Funder;
import eu.dnetlib.dhp.schema.dump.oaf.Projects;
import eu.dnetlib.dhp.schema.dump.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class SparkPrepareResultProject implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkPrepareResultProject.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkPrepareResultProject.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/project_prepare_parameters.json"));

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
			.filter("dataInfo.deletedbyinference = false and relClass = 'produces'");
		Dataset<Project> projects = Utils.readPath(spark, inputPath + "/project", Project.class);

		projects
			.joinWith(relation, projects.col("id").equalTo(relation.col("source")))
			.groupByKey(
				(MapFunction<Tuple2<Project, Relation>, String>) value -> value._2().getTarget(), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, Tuple2<Project, Relation>, ResultProject>) (s, it) -> {
				Set<String> projectSet = new HashSet<>();
				Tuple2<Project, Relation> first = it.next();
				ResultProject rp = new ResultProject();
				rp.setResultId(first._2().getTarget());
				Project p = first._1();
				projectSet.add(p.getId());
				Projects ps = Projects
					.newInstance(
						p.getId(), p.getCode().getValue(),
						Optional
							.ofNullable(p.getAcronym())
							.map(a -> a.getValue())
							.orElse(null),
						Optional
							.ofNullable(p.getTitle())
							.map(v -> v.getValue())
							.orElse(null),
						Optional
							.ofNullable(p.getFundingtree())
							.map(
								value -> value
									.stream()
									.map(ft -> getFunder(ft.getValue()))
									.collect(Collectors.toList())
									.get(0))
							.orElse(null));
				List<Projects> projList = new ArrayList<>();
				projList.add(ps);
				rp.setProjectsList(projList);
				it.forEachRemaining(c -> {
					Project op = c._1();
					if (!projectSet.contains(op.getId())) {
						projList
							.add(
								Projects
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
											.map(
												value -> value
													.stream()
													.map(ft -> getFunder(ft.getValue()))
													.collect(Collectors.toList())
													.get(0))
											.orElse(null)));
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
