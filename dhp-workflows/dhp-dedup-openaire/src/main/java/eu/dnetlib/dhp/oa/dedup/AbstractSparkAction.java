package eu.dnetlib.dhp.oa.dedup;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import scala.xml.Elem;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

abstract class AbstractSparkAction implements Serializable {

    public ArgumentApplicationParser parser;   //parameters for the spark action
    public ISLookUpService isLookUpService;    //lookup service to take dedupconfig

    public AbstractSparkAction(ArgumentApplicationParser parser, ISLookUpService isLookUpService) throws Exception {

        this.parser = parser;
        this.isLookUpService = isLookUpService;
    }

    public List<DedupConfig> getConfigurations(String orchestrator) throws ISLookUpException, DocumentException, IOException {

        final String xquery = String.format("/RESOURCE_PROFILE[.//DEDUPLICATION/ACTION_SET/@id = '%s']", orchestrator);

        String orchestratorProfile = isLookUpService.getResourceProfileByQuery(xquery);

        final Document doc = new SAXReader().read(new StringReader(orchestratorProfile));

        final String actionSetId = doc.valueOf("//DEDUPLICATION/ACTION_SET/@id");

        final List<DedupConfig> configurations = new ArrayList<>();

        for (final Object o : doc.selectNodes("//SCAN_SEQUENCE/SCAN")) {
            configurations.add(loadConfig(isLookUpService, actionSetId, o));
        }

        return configurations;
    }

    public DedupConfig loadConfig(final ISLookUpService isLookUpService, final String actionSetId, final Object o)
            throws ISLookUpException, IOException {
        final Element s = (Element) o;
        final String configProfileId = s.attributeValue("id");
        final String conf =
                isLookUpService.getResourceProfileByQuery(String.format(
                        "for $x in /RESOURCE_PROFILE[.//RESOURCE_IDENTIFIER/@value = '%s'] return $x//DEDUPLICATION/text()",
                        configProfileId));

        DedupConfig dedupConfig = new ObjectMapper().readValue(conf, DedupConfig.class);
        dedupConfig.getPace().initModel();
        dedupConfig.getPace().initTranslationMap();
        dedupConfig.getWf().setConfigurationId(actionSetId);

        return dedupConfig;
    }

    abstract void run() throws DocumentException, IOException, ISLookUpException;

    protected SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        return SparkSession
                .builder()
                .appName(SparkCreateSimRels.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .getOrCreate();
    }
}
