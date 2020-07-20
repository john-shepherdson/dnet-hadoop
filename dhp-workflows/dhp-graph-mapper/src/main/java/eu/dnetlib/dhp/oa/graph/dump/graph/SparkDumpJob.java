package eu.dnetlib.dhp.oa.graph.dump.graph;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.DumpProducts;
import eu.dnetlib.dhp.oa.graph.dump.QueryInformationSystem;
import eu.dnetlib.dhp.oa.graph.dump.ResultMapper;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.oa.graph.dump.community.SparkDumpCommunityProducts;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.ControlledField;
import eu.dnetlib.dhp.schema.dump.oaf.Country;
import eu.dnetlib.dhp.schema.dump.oaf.Result;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Organization;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkDumpJob implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SparkDumpCommunityProducts.class);

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        SparkDumpJob.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/dump/input_graphdump_parameters.json"));

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

        final String resultClassName = parser.get("resultTableName");
        log.info("resultTableName: {}", resultClassName);

        final String isLookUpUrl = parser.get("isLookUpUrl");
        log.info("isLookUpUrl: {}", isLookUpUrl);

        Class<? extends OafEntity> inputClazz = (Class<? extends OafEntity>) Class.forName(resultClassName);

        QueryInformationSystem queryInformationSystem = new QueryInformationSystem();
        queryInformationSystem.setIsLookUp(Utils.getIsLookUpService(isLookUpUrl));
        CommunityMap communityMap = queryInformationSystem.getCommunityMap();

        switch (ModelSupport.idPrefixMap.get(inputClazz)){
            case "50":
                DumpProducts d = new DumpProducts();
                d.run(isSparkSessionManaged,inputPath,outputPath,communityMap, inputClazz, true);
                break;
            case "40":

                break;
            case "20":
                SparkConf conf = new SparkConf();

                runWithSparkSession(
                        conf,
                        isSparkSessionManaged,
                        spark -> {
                            Utils.removeOutputDir(spark, outputPath);
                            organizationMap(spark, inputPath, outputPath);

                        });
                break;
        }




    }

    private static void organizationMap(SparkSession spark, String inputPath, String outputPath) {
        Utils.readPath(spark, inputPath, eu.dnetlib.dhp.schema.oaf.Organization.class)
                .map(o -> map(o), Encoders.bean(Organization.class))
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .json(outputPath);
    }

    private static Organization map(eu.dnetlib.dhp.schema.oaf.Organization org){
        Organization organization = new Organization();
        Optional.ofNullable(org.getLegalshortname())
                .ifPresent(value -> organization.setLegalshortname(value.getValue()));

        Optional.ofNullable(org.getLegalname())
                .ifPresent(value -> organization.setLegalname(value.getValue()));

        Optional.ofNullable(org.getWebsiteurl())
            .ifPresent(value -> organization.setWebsiteurl(value.getValue()));

        Optional.ofNullable(org.getAlternativeNames())
                .ifPresent(value -> organization.setAlternativenames(value.stream()
                .map( v-> v.getValue()).collect(Collectors.toList())));

        Optional.ofNullable(org.getCountry())
                .ifPresent(value -> organization.setCountry(Country.newInstance(value.getClassid(), value.getClassname(), null)));

        Optional.ofNullable(org.getId())
                .ifPresent(value -> organization.setId(value));

        Optional.ofNullable(org.getPid())
                .ifPresent(value -> organization.setPid(
                        value.stream().map(p -> ControlledField.newInstance(p.getQualifier().getClassid(), p.getValue())).collect(Collectors.toList())
                ));

        return organization;
    }

}
