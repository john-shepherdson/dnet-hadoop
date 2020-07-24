package eu.dnetlib.dhp.oa.graph.dump.graph;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

import eu.dnetlib.dhp.oa.graph.dump.Utils;

import eu.dnetlib.dhp.schema.dump.oaf.graph.ResearchCommunity;
import eu.dnetlib.dhp.schema.dump.oaf.graph.ResearchInitiative;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;

public class SparkCreateContextEntities implements Serializable {

    //leggo i context dall'is e mi faccio la mappa id -> contextinfo

    //creo le entities con le info generali

    //creo le relazioni con le info in projectList e datasourceList. Le relazioni sono di tipo isRelatedTo da una parte e dall'altra

    //prendo un parametro community_organization per creare relazioni fra community ed organizzazioni . n.b. l'id dell'organizzazione
    //e' non deduplicato => bisogna risolverlo e prendere i dedup id distinti

    private static final Logger log = LoggerFactory.getLogger(SparkCreateContextEntities.class);

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        SparkCreateContextEntities.class
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

        final String isLookUpUrl = parser.get("isLookUpUrl");
        log.info("isLookUpUrl: {}", isLookUpUrl);

        QueryInformationSystem queryInformationSystem = new QueryInformationSystem();
        queryInformationSystem.setIsLookUp(Utils.getIsLookUpService(isLookUpUrl));
        CommunityMap communityMap = queryInformationSystem.getCommunityMap();

        createEntities(communityMap, outputPath + "/context");

    }

    private static void createEntities(CommunityMap communityMap, String s) {
        communityMap.keySet().stream()
                .map(key -> {
                    ResearchInitiative community;
                    ContextInfo cinfo = communityMap.get(key);
                    if(cinfo.getType().equals("community")){
                        community = new ResearchCommunity();
                    }else{
                        community = new ResearchInitiative();
                    }
                    return community;
                })
    }


}
