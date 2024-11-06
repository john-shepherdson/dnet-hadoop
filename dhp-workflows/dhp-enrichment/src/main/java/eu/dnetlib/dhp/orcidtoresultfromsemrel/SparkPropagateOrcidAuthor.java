package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.author.SparkEnrichWithOrcidAuthors;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.utils.OrcidAuthor;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;


public class SparkPropagateOrcidAuthor extends SparkEnrichWithOrcidAuthors {
    private static final Logger log = LoggerFactory.getLogger(SparkPropagateOrcidAuthor.class);

    public SparkPropagateOrcidAuthor(String propertyPath, String[] args, Logger log) {
        super(propertyPath, args, log);
    }

    public static void main(String[] args) throws Exception {

        // Create instance and run the Spark application
        SparkPropagateOrcidAuthor app = new SparkPropagateOrcidAuthor("/eu/dnetlib/dhp/wf/subworkflows/orcidtoresultfromsemrel/input_orcidtoresult_parameters.json", args, log);
        app.initialize().run();

    }

    private static OrcidAuthors getOrcidAuthorsList(List<Author> authors) {
        OrcidAuthors oas = new OrcidAuthors();
        List<OrcidAuthor> tmp = authors.stream().map(SparkPropagateOrcidAuthor::getOrcidAuthor)
                .filter(Objects::nonNull).collect(Collectors.toList());
        oas.setOrcidAuthorList(tmp);
        return oas;
    }

    private static OrcidAuthor getOrcidAuthor(Author a){
        return Optional.ofNullable(getOrcid(a))
                .map(orcid -> new OrcidAuthor(orcid,a.getSurname(), a.getName(), a.getFullname(), null))
                .orElse(null);

    }

    private static String getOrcid(Author a){
        if (a.getPid().stream().anyMatch(p->p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID)))
            return a.getPid().stream().filter(p->p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID)).findFirst().get().getValue();
        if (a.getPid().stream().anyMatch(p->p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID_PENDING)))
            return a.getPid().stream().filter(p->p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID_PENDING)).findFirst().get().getValue();
        return null;

    }

    @Override
    public void createTemporaryData(SparkSession spark, String graphPath, String orcidPath, String targetPath) {
        ModelSupport.entityTypes
                .keySet().stream().filter(ModelSupport::isResult)
                .forEach(e -> {
                    Dataset<Row> orcidDnet = spark.read().schema(Encoders.bean(Result.class).schema())
                            .json(graphPath + "/"+ e.name())
                            .as(Encoders.bean(Result.class))
                            .filter((FilterFunction<Result>) r -> r.getAuthor().stream()
                                    .anyMatch(a -> a.getPid()
                                            .stream()
                                            .anyMatch(p -> p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID) ||
                                                    p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID_PENDING))))
                            .map((MapFunction<Result, Tuple2<String, OrcidAuthors>>) r ->
                                            new Tuple2<>(r.getId(), getOrcidAuthorsList(r.getAuthor()))
                                    , Encoders.tuple(Encoders.STRING(), Encoders.bean(OrcidAuthors.class)))
                            .selectExpr("_1 as target", "_2 as orcid_authors");

                    Dataset<Row> result = spark.read().schema(Encoders.bean(Result.class).schema())
                            .json(graphPath + "/"+ e.name())
                            .as(Encoders.bean(Result.class))
                            .selectExpr("id", "author as graph_authors");

                    Dataset<Row> supplements = spark.read()
                            .schema(Encoders.bean(Relation.class).schema())
                            .json(graphPath + "/"+ "relation")
                            .where("relclass IN('" + ModelConstants.IS_SUPPLEMENT_TO + "', '" +
                                    ModelConstants.IS_SUPPLEMENTED_BY + "')")
                            .selectExpr("source as id", "target");

                    result
                            .join(supplements, "id")
                            .join(orcidDnet, "target")
                            .drop("target")
                            .write()
                            .mode(SaveMode.Overwrite)
                            .option("compression", "gzip")
                            .parquet(targetPath + "/"+ e.name() + "_unmatched");

                });
    }
}
