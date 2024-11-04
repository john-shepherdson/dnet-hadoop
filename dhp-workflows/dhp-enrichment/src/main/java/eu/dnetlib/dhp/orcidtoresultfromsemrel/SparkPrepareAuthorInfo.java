package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.utils.OrcidAuthor;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.PropagationConstant.isSparkSessionManaged;
import static eu.dnetlib.dhp.PropagationConstant.removeOutputDir;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkPrepareAuthorInfo {
    private static final Logger log = LoggerFactory.getLogger(SparkPrepareAuthorInfo.class);

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        SparkPrepareAuthorInfo.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/wf/subworkflows/orcidtoresultfromsemrel/input_orcidtoresult_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
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
                    removeOutputDir(spark, outputPath);
                    createTemporaryData(spark, inputPath, outputPath);

                    }
                );
    }

    private static void createTemporaryData(SparkSession spark, String inputPath, String outputPath) {
        ModelSupport.entityTypes
                .keySet().stream().filter(ModelSupport::isResult)
                .forEach(e -> {
                    Dataset<Row> orcidDnet = spark.read().schema(Encoders.bean(Result.class).schema())
                            .json(inputPath + e.name())
                            .as(Encoders.bean(Result.class))
                            .filter((FilterFunction<Result>) r -> r.getAuthor().stream()
                                    .anyMatch(a -> a.getPid()
                                            .stream()
                                            .anyMatch(p -> p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID) ||
                                                    p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID_PENDING))))
                            .map((MapFunction<Result, Tuple2<String, OrcidAuthors>>) r ->
                                            new Tuple2<>(r.getId(), getOrcidAuthorsList(r.getAuthor()))
                                    , Encoders.tuple(Encoders.STRING(), Encoders.bean(OrcidAuthors.class)))
                            .selectExpr("_1 as id", "_2 as orcid_authors");

                    Dataset<Row> result = spark.read().schema(Encoders.bean(Result.class).schema())
                            .json(inputPath + e.name())
                            .as(Encoders.bean(Result.class))
                            .selectExpr("id", "author as graph_authors");

                    Dataset<Row> supplements = spark.read()
                            .schema(Encoders.bean(Relation.class).schema())
                            .json(inputPath + "relation")
                            .where("relclass IN('" + ModelConstants.IS_SUPPLEMENT_TO + "', '" +
                                    ModelConstants.IS_SUPPLEMENTED_BY + "')")
                            .selectExpr("source as id", "target");

                    result
                            .join(supplements, "id")
                            .join(orcidDnet, orcidDnet.col("id").equalTo(supplements.col("target")))
                            .drop("target")
                            .write()
                            .mode(SaveMode.Overwrite)
                            .option("compression", "gzip")
                            .parquet(outputPath + e.name() + "_unmatched");

                });


    }



//        override def createTemporaryData(graphPath: String, orcidPath: String, targetPath: String): Unit = {
//                val relEnc = Encoders.bean(classOf[Relation])
//
//                ModelSupport.entityTypes.asScala
//                        .filter(e => ModelSupport.isResult(e._1))
//      .foreach(e => {
//                val resultType = e._1.name()
//                val enc = Encoders.bean(e._2)
//
//                val orcidDnet = spark.read
//                .load("$graphPath/$resultType")
//                .as[Result]
//                .map(
//                        result =>
//                (
//                        result.getId,
//                result.getAuthor.asScala.map(a => OrcidAuthor("extract ORCID", a.getSurname, a.getName, a.getFullname, null))
//            )
//          )
//          .where("size(_2) > 0")
//                .selectExpr("_1 as id", "_2 as orcid_authors")
//
//        val result =
//                spark.read.schema(enc.schema).json(s"$graphPath/$resultType").selectExpr("id", "author as graph_authors")
//
//        val supplements = spark.read.schema(relEnc.schema).json(s"$graphPath/relation").where("relclass IN('isSupplementedBy', 'isSupplementOf')").selectExpr("source as id", "target")
//
//        result
//                .join(supplements, Seq("id"))
//                .join(orcidDnet, orcidDnet("id") === col("target"))
//                .drop("target")
//                .write
//                .mode(SaveMode.Overwrite)
//                .option("compression", "gzip")
//                .parquet(s"$targetPath/${resultType}_unmatched")
//      })
//  }

    private static OrcidAuthors getOrcidAuthorsList(List<Author> authors) {
        OrcidAuthors oas = new OrcidAuthors();
        List<OrcidAuthor> tmp = authors.stream().map(SparkPrepareAuthorInfo::getOrcidAuthor).collect(Collectors.toList());
        oas.setOrcidAuthorList(tmp);
        return oas;
    }

    private static OrcidAuthor getOrcidAuthor(Author a){
        return new OrcidAuthor(getOrcid(a),a.getSurname(), a.getName(), a.getFullname(), null);

    }

    private static String getOrcid(Author a){
        if (a.getPid().stream().anyMatch(p->p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID)))
            return a.getPid().stream().filter(p->p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID)).findFirst().get().getValue();
        return a.getPid().stream().filter(p->p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID_PENDING)).findFirst().get().getValue();

    }
}
