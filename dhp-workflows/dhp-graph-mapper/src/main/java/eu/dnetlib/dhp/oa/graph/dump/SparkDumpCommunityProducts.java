package eu.dnetlib.dhp.oa.graph.dump;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Result;

import javax.management.Query;


public class SparkDumpCommunityProducts implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SparkDumpCommunityProducts.class);
    private QueryInformationSystem queryInformationSystem;


    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        SparkDumpCommunityProducts.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/dump/input_parameters.json"));

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

        final String dumpClassName = parser.get("dumpTableName");
        log.info("dumpClassName: {}", dumpClassName);

        final String isLookUpUrl = parser.get("isLookUpUrl");
        log.info("isLookUpUrl: {}", isLookUpUrl);

        final String resultType = parser.get("resultType");
        log.info("resultType: {}", resultType);


        SparkDumpCommunityProducts sdcp = new SparkDumpCommunityProducts();

        sdcp.exec(isLookUpUrl, isSparkSessionManaged, outputPath,
                inputPath, resultClassName, dumpClassName);

    }

    public QueryInformationSystem getQueryInformationSystem() {
        return queryInformationSystem;
    }

    public void setQueryInformationSystem(QueryInformationSystem queryInformationSystem) {
        this.queryInformationSystem = queryInformationSystem;
    }

    public ISLookUpService getIsLookUpService(String isLookUpUrl){
        return ISLookupClientFactory.getLookUpService(isLookUpUrl);
    }

    public void exec(String isLookUpUrl, Boolean isSparkSessionManaged, String outputPath, String inputPath,
                     String resultClassName, String dumpClassName) throws ISLookUpException, ClassNotFoundException {
        SparkConf conf = new SparkConf();

        Class<? extends Result> inputClazz = (Class<? extends Result>) Class.forName(resultClassName);
        Class<? extends eu.dnetlib.dhp.schema.dump.oaf.Result> dumpClazz =
                (Class<? extends eu.dnetlib.dhp.schema.dump.oaf.Result>) Class.forName(dumpClassName);

        queryInformationSystem.setIsLookUp(getIsLookUpService(isLookUpUrl));
        Map<String,String>
                communityMap = queryInformationSystem.getCommunityMap();
        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    Utils.removeOutputDir(spark, outputPath);
                    execDump(spark, inputPath, outputPath , communityMap, inputClazz, dumpClazz);
                });
    }


    private  <I extends Result, O extends eu.dnetlib.dhp.schema.dump.oaf.Result > void execDump(
            SparkSession spark,
            String inputPath,
            String outputPath,
            Map<String,String> communityMap,
            Class<I> inputClazz,
            Class<O> dumpClazz) {

        Set<String> communities = communityMap.keySet();
        Dataset<I> tmp = Utils.readPath(spark, inputPath, inputClazz);
        tmp.map(value -> {
            Optional<List<Context>> inputContext = Optional.ofNullable(value.getContext());
            if(!inputContext.isPresent()){
                return null;
            }
            List<String> toDumpFor = inputContext.get().stream().map(c -> {
                if (communities.contains(c.getId())) {
                    return c.getId();
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
            if(toDumpFor.size() == 0){
                return null;
            }
            return Mapper.map(value, communityMap);
        },Encoders.bean(dumpClazz))
        .write()
        .mode(SaveMode.Overwrite)
        .option("compression","gzip")
        .json(outputPath);

    }


}

