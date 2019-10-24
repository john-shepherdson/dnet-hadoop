package eu.dnetlib.dhp.collection.worker;


import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginFactory;
import eu.dnetlib.message.MessageManager;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DnetCollectortWorkerApplication is the main class responsible to start
 * the Dnet Collection into HDFS.
 * This module will be executed on the hadoop cluster and taking in input some parameters
 * that tells it which is the right collector plugin to use  and where store the data into HDFS path
 *
 * @author Sandro La Bruzzo
 */

public class DnetCollectorWorkerApplication {

    private static final Logger log = LoggerFactory.getLogger(DnetCollectorWorkerApplication.class);

    private static CollectorPluginFactory collectorPluginFactory = new CollectorPluginFactory();

    private static ArgumentApplicationParser argumentParser;


    /**
     * @param args
     */
    public static void main(final String[] args) throws Exception {

        argumentParser= new ArgumentApplicationParser(IOUtils.toString(DnetCollectorWorker.class.getResourceAsStream("/eu/dnetlib/collector/worker/collector_parameter.json")));
        argumentParser.parseArgument(args);
        log.info("hdfsPath =" + argumentParser.get("hdfsPath"));
        log.info("json = " + argumentParser.get("apidescriptor"));
        final MessageManager manager = new MessageManager(argumentParser.get("rabbitHost"), argumentParser.get("rabbitUser"), argumentParser.get("rabbitPassword"), false, false, null);
        final DnetCollectorWorker worker = new DnetCollectorWorker(collectorPluginFactory, argumentParser, manager);
        worker.collect();


    }


}
