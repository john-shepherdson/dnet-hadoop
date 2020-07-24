package eu.dnetlib.dhp.oa.graph.dump.graph;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.DumpProducts;
import eu.dnetlib.dhp.oa.graph.dump.QueryInformationSystem;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.oa.graph.dump.community.SparkDumpCommunityProducts;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.ControlledField;
import eu.dnetlib.dhp.schema.dump.oaf.Country;
import eu.dnetlib.dhp.schema.dump.oaf.graph.*;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.html.Option;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
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

        final String isLookUpUrl = parser.get("isLookUpUrl");
        log.info("isLookUpUrl: {}", isLookUpUrl);

        Class<? extends OafEntity> inputClazz = (Class<? extends OafEntity>) Class.forName(resultClassName);

        QueryInformationSystem queryInformationSystem = new QueryInformationSystem();
        queryInformationSystem.setIsLookUp(Utils.getIsLookUpService(isLookUpUrl));
        CommunityMap communityMap = queryInformationSystem.getCommunityMap();

        DumpGraph dg = new DumpGraph();
        dg.run(isSparkSessionManaged, inputPath, outputPath, inputClazz, communityMap);


    }



}
