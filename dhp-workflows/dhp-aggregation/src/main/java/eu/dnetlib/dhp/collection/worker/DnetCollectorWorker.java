package eu.dnetlib.dhp.collection.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.collector.worker.model.ApiDescriptor;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginFactory;
import eu.dnetlib.message.Message;
import eu.dnetlib.message.MessageManager;
import eu.dnetlib.message.MessageType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DnetCollectorWorker {

    private static final Logger log = LoggerFactory.getLogger(DnetCollectorWorker.class);


    private final CollectorPluginFactory collectorPluginFactory;

    private final ArgumentApplicationParser argumentParser;

    private final MessageManager manager;


    public DnetCollectorWorker(final CollectorPluginFactory collectorPluginFactory, final ArgumentApplicationParser argumentParser, final MessageManager manager) throws DnetCollectorException {
        this.collectorPluginFactory = collectorPluginFactory;
        this.argumentParser = argumentParser;
        this.manager = manager;
    }


    public void collect() throws DnetCollectorException {
        try {
            final ObjectMapper jsonMapper = new ObjectMapper();
            final ApiDescriptor api = jsonMapper.readValue(argumentParser.get("apidescriptor"), ApiDescriptor.class);

            final CollectorPlugin plugin = collectorPluginFactory.getPluginByProtocol(api.getProtocol());

            final String hdfsuri = argumentParser.get("namenode");

            // ====== Init HDFS File System Object
            Configuration conf = new Configuration();
            // Set FileSystem URI
            conf.set("fs.defaultFS", hdfsuri);
            // Because of Maven
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            System.setProperty("HADOOP_USER_NAME", argumentParser.get("userHDFS"));
            System.setProperty("hadoop.home.dir", "/");
            //Get the filesystem - HDFS
            FileSystem.get(URI.create(hdfsuri), conf);
            Path hdfswritepath = new Path(argumentParser.get("hdfsPath"));

            log.info("Created path " + hdfswritepath.toString());

            final Map<String, String> ongoingMap = new HashMap<>();
            final Map<String, String> reportMap = new HashMap<>();
            final AtomicInteger counter = new AtomicInteger(0);
            try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(hdfswritepath), SequenceFile.Writer.keyClass(IntWritable.class),
                    SequenceFile.Writer.valueClass(Text.class))) {
                final IntWritable key = new IntWritable(counter.get());
                final Text value = new Text();
                plugin.collect(api).forEach(content -> {

                    key.set(counter.getAndIncrement());
                    value.set(content);
                    if (counter.get() % 10 == 0) {
                        try {
                            ongoingMap.put("ongoing", "" + counter.get());
                            log.debug("Sending message: "+ manager.sendMessage(new Message(argumentParser.get("workflowId"), "Collection", MessageType.ONGOING, ongoingMap), argumentParser.get("rabbitOngoingQueue"), true, false));
                        } catch (Exception e) {
                            log.error("Error on sending message ", e);
                        }
                    }
                    try {
                        writer.append(key, value);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                });
            }
            ongoingMap.put("ongoing", "" + counter.get());
            manager.sendMessage(new Message(argumentParser.get("workflowId"), "Collection", MessageType.ONGOING, ongoingMap), argumentParser.get("rabbitOngoingQueue"), true, false);
            reportMap.put("collected", "" + counter.get());
            manager.sendMessage(new Message(argumentParser.get("workflowId"), "Collection", MessageType.REPORT, reportMap), argumentParser.get("rabbitOngoingQueue"), true, false);
            manager.close();
        } catch (Throwable e) {
            throw new DnetCollectorException("Error on collecting ",e);
        }
    }





}
