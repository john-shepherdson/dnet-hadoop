package eu.dnetlib.dhp.oa.dedup;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

abstract class AbstractSparkAction implements Serializable {

    public ArgumentApplicationParser parser;   //parameters for the spark action
    public SparkSession spark; //the spark session

    public AbstractSparkAction(ArgumentApplicationParser parser, SparkSession spark) throws Exception {

        this.parser = parser;
        this.spark = spark;
    }

    public List<DedupConfig> getConfigurations(ISLookUpService isLookUpService, String orchestrator) throws ISLookUpException, DocumentException, IOException {

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

    private DedupConfig loadConfig(final ISLookUpService isLookUpService, final String actionSetId, final Object o)
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

    abstract void run(ISLookUpService isLookUpService) throws DocumentException, IOException, ISLookUpException;

    protected static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[] {
                Author.class,
                Context.class,
                Country.class,
                DataInfo.class,
                Dataset.class,
                Datasource.class,
                ExternalReference.class,
                ExtraInfo.class,
                Field.class,
                GeoLocation.class,
                Instance.class,
                Journal.class,
                KeyValue.class,
                Oaf.class,
                OafEntity.class,
                OAIProvenance.class,
                Organization.class,
                OriginDescription.class,
                OtherResearchProduct.class,
                Project.class,
                Publication.class,
                Qualifier.class,
                Relation.class,
                Result.class,
                Software.class,
                StructuredProperty.class
        });

        return SparkSession
                .builder()
                .appName(SparkCreateSimRels.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .getOrCreate();
    }
}
