package eu.dnetlib.dhp.oa.graph.fix;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils;
import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class FixGraphProperties {

    private static final Logger log = LoggerFactory.getLogger(FixGraphProperties.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils
                .toString(
                        FixGraphProperties.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/input_fix_graph_parameters.json"));
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("inputPath");
        log.info("inputPath: {}", inputPath);

        String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        String isLookupUrl = parser.get("isLookupUrl");
        log.info("isLookupUrl: {}", isLookupUrl);

        String graphTableClassName = parser.get("graphTableClassName");
        log.info("graphTableClassName: {}", graphTableClassName);

        Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

        final VocabularyGroup vocs = VocabularyGroup.loadVocsFromIS(isLookupUrl);

        SparkConf conf = new SparkConf();
        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    removeOutputDir(spark, outputPath);
                    fixGraphTable(spark, vocs, inputPath, entityClazz, outputPath);
                });
    }

    private static <T extends Oaf> void fixGraphTable(
            SparkSession spark,
            VocabularyGroup vocs,
            String inputPath,
            Class<T> clazz,
            String outputPath) {

        MapFunction<T, T> fixFn = getFixingFunction(vocs, clazz);

        readTableFromPath(spark, inputPath, clazz)
                .map(fixFn, Encoders.bean(clazz))
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(outputPath);
    }

    private static <T extends Oaf> MapFunction<T, T> getFixingFunction(VocabularyGroup vocs, Class<T> clazz) {

        switch (clazz.getCanonicalName()) {
            case "eu.dnetlib.dhp.schema.oaf.Publication":
            case "eu.dnetlib.dhp.schema.oaf.Dataset":
            case "eu.dnetlib.dhp.schema.oaf.OtherResearchProduct":
            case "eu.dnetlib.dhp.schema.oaf.Software":
                return (MapFunction<T, T>) value -> {
                    Result r = (Result) value;

                    if (r.getLanguage() != null) {
                        r.setLanguage(vocs.getTermAsQualifier("dnet:languages", "und"));
                    } else {
                        r.setLanguage(vocs.getTermAsQualifier("dnet:languages", r.getLanguage().getClassid()));
                    }
                    if (r.getCountry() != null) {
                        r.setCountry(
                                r.getCountry()
                                .stream()
                                .filter(Objects::nonNull)
                                .map(c -> {
                                    Qualifier q = vocs.getTermAsQualifier("dnet:countries", c.getClassid());
                                    Country cn = new Country();
                                    cn.setDataInfo(c.getDataInfo());
                                    cn.setClassid(q.getClassid());
                                    cn.setClassname(cn.getClassname());
                                    cn.setSchemeid("dnet:countries");
                                    cn.setSchemename("dnet:countries");
                                    return cn;
                                })
                                .collect(Collectors.toList()));
                    }

                    if (r.getSubject() != null) {
                        r.setSubject(
                                r.getSubject()
                                .stream()
                                .filter(Objects::nonNull)
                                .map(s -> {
                                    if (s.getQualifier() == null || StringUtils.isBlank(s.getQualifier().getClassid())) {
                                        s.setQualifier(vocs.getTermAsQualifier("dnet:subject_classification_typologies", "UNKNOWN"));
                                    }
                                })
                                .collect(Collectors.toList())
                        );
                    }

                    if (r.getPublisher() != null && StringUtils.isBlank(r.getPublisher().getValue())) {
                        r.setPublisher(null);
                    }
                    if (r.getBestaccessright() == null) {
                        r.setBestaccessright(vocs.getTermAsQualifier("dnet:access_modes", "UNKNOWN"));
                    }
                    if (r.getInstance() != null) {
                        for(Instance i : r.getInstance()) {
                            if (i.getAccessright() == null) {
                                i.setAccessright(vocs.getTermAsQualifier("dnet:access_modes", "UNKNOWN"));
                            }
                            if (i.getInstancetype() != null) {
                                i.setInstancetype(vocs.getTermAsQualifier("dnet:publication_resource", i.getInstancetype().getClassid()));
                            } else {
                                i.setInstancetype(vocs.getTermAsQualifier("dnet:publication_resource", "0000"));
                            }


                        }
                    }

                    return clazz.cast(r);
                };
            case "eu.dnetlib.dhp.schema.oaf.Datasource":
                return (MapFunction<T, T>) value -> {
                    return value;
                };
            case "eu.dnetlib.dhp.schema.oaf.Organization":
                return (MapFunction<T, T>) value -> {
                    Organization o = (Organization) value;

                    if (o.getCountry() == null) {
                        o.setCountry(vocs.getTermAsQualifier("dnet:countries", "UNKNOWN"));
                    } else {
                        o.setCountry(vocs.getTermAsQualifier("dnet:countries", o.getCountry().getClassid()));
                    }

                    return clazz.cast(o);
                };
            case "eu.dnetlib.dhp.schema.oaf.Project":
                return (MapFunction<T, T>) value -> {
                    return value;
                };
            case "eu.dnetlib.dhp.schema.oaf.Relation":
                return (MapFunction<T, T>) value -> {
                    return value;
                };
            default:
                throw new RuntimeException("unknown class: " + clazz.getCanonicalName());
        }

    }

    private static <T extends Oaf> Dataset<T> readTableFromPath(
            SparkSession spark, String inputEntityPath, Class<T> clazz) {

        log.info("Reading Graph table from: {}", inputEntityPath);
        return spark
                .read()
                .textFile(inputEntityPath)
                .map(
                        (MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, clazz),
                        Encoders.bean(clazz));
    }

    private static void removeOutputDir(SparkSession spark, String path) {
        HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
    }

}
