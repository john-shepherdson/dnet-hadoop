package eu.dnetlib.dhp.oa.dedup;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.commons.io.IOUtils;
import org.dom4j.DocumentException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
public class SparkDedupTest {

    ISLookUpService isLookUpService = mock(ISLookUpService.class, withSettings().serializable());


    @BeforeEach
    public void setUp() throws IOException, ISLookUpException, DocumentException {

        withSettings().serializable();

        when(isLookUpService.getResourceProfileByQuery(Mockito.contains("test-orchestrator")))
                .thenReturn(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator.xml")));

        when(isLookUpService.getResourceProfileByQuery(Mockito.contains("organization")))
                .thenReturn(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));

        when(isLookUpService.getResourceProfileByQuery(Mockito.contains("publication")))
                .thenReturn(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/pub.curr.conf.json")));
    }

    @Test
    public void createSimRelsTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json")));
        parser.parseArgument(new String[]{
                "-mt", "local[*]",
                "-i", "/Users/miconis/dumps",
                "-asi", "test-orchestrator",
                "-la", "lookupurl",
                "-w", "workingPath"});

        new SparkCreateSimRels(parser, isLookUpService).run();

    }

//    @Disabled("must be parametrized to run locally")
//    public void createCCTest() throws Exception {
//
//        SparkCreateConnectedComponent.main(new String[]{
//                "-mt", "local[*]",
//                "-s", "/Users/miconis/dumps",
//                "-e", entity,
//                "-c", ArgumentApplicationParser.compressArgument(configuration),
//                "-t", "/tmp/dedup",
//        });
//    }
//
//    @Disabled("must be parametrized to run locally")
//    public void dedupRecordTest() throws Exception {
//        SparkCreateDedupRecord.main(new String[]{
//                "-mt", "local[*]",
//                "-s", "/Users/miconis/dumps",
//                "-e", entity,
//                "-c", ArgumentApplicationParser.compressArgument(configuration),
//                "-d", "/tmp/dedup",
//        });
//    }
//
//    @Disabled("must be parametrized to run locally")
//    public void printConfiguration() throws Exception {
//        System.out.println(ArgumentApplicationParser.compressArgument(configuration));
//    }

    @Disabled("must be parametrized to run locally")
    public void testHashCode() {
        final String s1 = "20|grid________::6031f94bef015a37783268ec1e75f17f";
        final String s2 = "20|nsf_________::b12be9edf414df8ee66b4c52a2d8da46";

        final HashFunction hashFunction = Hashing.murmur3_128();

        System.out.println(s1.hashCode());
        System.out.println(hashFunction.hashString(s1).asLong());
        System.out.println(s2.hashCode());
        System.out.println(hashFunction.hashString(s2).asLong());
    }

    public List<DedupConfig> prepareConfigurations() throws IOException {

        return Lists.newArrayList(
                DedupConfig.load(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json"))),
                DedupConfig.load(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")))
        );
    }
}
