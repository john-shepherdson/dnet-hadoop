package eu.dnetlib.dhp.provision.update;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.provision.scholix.Scholix;
import eu.dnetlib.dhp.provision.scholix.ScholixIdentifier;
import eu.dnetlib.dhp.provision.scholix.ScholixRelationship;
import eu.dnetlib.dhp.provision.scholix.ScholixResource;
import eu.dnetlib.dhp.utils.DHPUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

public class SparkResolveScholixTarget {

  public static void main(String[] args) throws Exception {
    final ArgumentApplicationParser parser =
        new ArgumentApplicationParser(
            IOUtils.toString(
                SparkResolveScholixTarget.class.getResourceAsStream(
                    "/eu/dnetlib/dhp/provision/input_resolve_scholix_parameters.json")));
    parser.parseArgument(args);

    final SparkConf conf = new SparkConf();

    final String master = parser.get("master");
    final String sourcePath = parser.get("sourcePath");
    final String workingDirPath = parser.get("workingDirPath");
    final String indexHost = parser.get("indexHost");
    try (SparkSession spark = getSession(conf, master)) {

      final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

      spark
          .createDataset(
              sc.sequenceFile(sourcePath, IntWritable.class, Text.class)
                  .map(Tuple2::_2)
                  .map(s -> new ObjectMapper().readValue(s.toString(), Scholix.class))
                  .rdd(),
              Encoders.bean(Scholix.class))
          .write()
          .save(workingDirPath + "/stepA");

      Dataset<Scholix> s1 =
          spark.read().load(workingDirPath + "/stepA").as(Encoders.bean(Scholix.class));

      s1.where(s1.col("target.dnetIdentifier").isNull())
          .select(s1.col("target.identifier"))
          .distinct()
          .map(
              (MapFunction<Row, ScholixResource>)
                  f -> {
                    final String pid = ((Row) f.getList(0).get(0)).getString(0);
                    ScholixResource publication =
                        new CrossrefClient(indexHost).getResourceByDOI(pid);
                    if (publication != null) {
                      return publication;
                    }
                    ScholixResource dataset = new DataciteClient(indexHost).getDatasetByDOI(pid);
                    if (dataset != null) {
                      return dataset;
                    }
                    ScholixResource r = new ScholixResource();
                    r.setIdentifier(Collections.singletonList(new ScholixIdentifier(pid, "doi")));
                    r.setObjectType("unknown");
                    r.setDnetIdentifier(
                        "70|" + DHPUtils.md5(String.format("%s::doi", pid.toLowerCase().trim())));

                    return r;
                  },
              Encoders.bean(ScholixResource.class))
          .write()
          .mode(SaveMode.Overwrite)
          .save(workingDirPath + "/stepB");

      Dataset<ScholixResource> s2 =
          spark.read().load(workingDirPath + "/stepB").as(Encoders.bean(ScholixResource.class));

      s1.joinWith(
              s2,
              s1.col("target.identifier.identifier").equalTo(s2.col("identifier.identifier")),
              "left")
          .flatMap(
              (FlatMapFunction<Tuple2<Scholix, ScholixResource>, Scholix>)
                  f -> {
                    final List<Scholix> res = new ArrayList<>();
                    final Scholix s = f._1();
                    final ScholixResource target = f._2();
                    if (StringUtils.isNotBlank(s.getIdentifier())) res.add(s);
                    else if (target == null) {
                      ScholixResource currentTarget = s.getTarget();
                      currentTarget.setObjectType("unknown");
                      currentTarget.setDnetIdentifier(
                          Datacite2Scholix.generateId(
                              currentTarget.getIdentifier().get(0).getIdentifier(),
                              currentTarget.getIdentifier().get(0).getSchema(),
                              currentTarget.getObjectType()));

                      s.generateIdentifier();
                      res.add(s);
                      final Scholix inverse = new Scholix();
                      inverse.setTarget(s.getSource());
                      inverse.setSource(s.getTarget());
                      inverse.setLinkprovider(s.getLinkprovider());
                      inverse.setPublicationDate(s.getPublicationDate());
                      inverse.setPublisher(s.getPublisher());
                      inverse.setRelationship(
                          new ScholixRelationship(
                              s.getRelationship().getInverse(),
                              s.getRelationship().getSchema(),
                              s.getRelationship().getName()));
                      inverse.generateIdentifier();
                      res.add(inverse);

                    } else {
                      target.setIdentifier(
                          target.getIdentifier().stream()
                              .map(
                                  d ->
                                      new ScholixIdentifier(
                                          d.getIdentifier().toLowerCase(),
                                          d.getSchema().toLowerCase()))
                              .collect(Collectors.toList()));
                      s.setTarget(target);
                      s.generateIdentifier();
                      res.add(s);
                      final Scholix inverse = new Scholix();
                      inverse.setTarget(s.getSource());
                      inverse.setSource(s.getTarget());
                      inverse.setLinkprovider(s.getLinkprovider());
                      inverse.setPublicationDate(s.getPublicationDate());
                      inverse.setPublisher(s.getPublisher());
                      inverse.setRelationship(
                          new ScholixRelationship(
                              s.getRelationship().getInverse(),
                              s.getRelationship().getSchema(),
                              s.getRelationship().getName()));
                      inverse.generateIdentifier();
                      res.add(inverse);
                    }

                    return res.iterator();
                  },
              Encoders.bean(Scholix.class))
          .javaRDD()
          .map(s -> new ObjectMapper().writeValueAsString(s))
          .saveAsTextFile(workingDirPath + "/resolved_json");
    }
  }

  private static SparkSession getSession(SparkConf conf, String master) {
    return SparkSession.builder()
        .config(conf)
        .appName(SparkResolveScholixTarget.class.getSimpleName())
        .master(master)
        .getOrCreate();
  }
}
