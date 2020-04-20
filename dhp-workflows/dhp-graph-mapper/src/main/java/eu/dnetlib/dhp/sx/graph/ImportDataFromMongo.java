package eu.dnetlib.dhp.sx.graph;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * This job is responsible to collect data from mongoDatabase and store in a sequence File on HDFS
 * Mongo database contains information of each MDSTore in two collections: -metadata That contains
 * info like: ID, format, layout, interpretation -metadataManager: that contains info : ID,
 * mongoCollectionName from the metadata collection we filter the ids with Format, layout, and
 * Interpretation from the metadataManager we get the current MONGO collection name which contains
 * metadata XML see function getCurrentId
 *
 * <p>This Job will be called different times in base at the triple we want import, and generates
 * for each triple a sequence file of XML
 */
public class ImportDataFromMongo {
    /**
     * It requires in input some parameters described on a file
     * eu/dnetlib/dhp/graph/sx/import_from_mongo_parameters.json
     *
     * <p>- the name node - the paht where store HDFS File - the mongo host - the mongo port - the
     * metadata format to import - the metadata layout to import - the metadata interpretation to
     * import - the mongo database Name
     *
     * <p>This params are encoded into args
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                ImportDataFromMongo.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/sx/graph/argumentparser/import_from_mongo_parameters.json")));
        parser.parseArgument(args);
        final int port = Integer.parseInt(parser.get("dbport"));
        final String host = parser.get("dbhost");

        final String format = parser.get("format");
        final String layout = parser.get("layout");
        final String interpretation = parser.get("interpretation");

        final String dbName = parser.get("dbName");
        final MongoClient client = new MongoClient(host, port);
        MongoDatabase database = client.getDatabase(dbName);

        MongoCollection<Document> metadata = database.getCollection("metadata");
        MongoCollection<Document> metadataManager = database.getCollection("metadataManager");
        final DBObject query =
                QueryBuilder.start("format")
                        .is(format)
                        .and("layout")
                        .is(layout)
                        .and("interpretation")
                        .is(interpretation)
                        .get();
        final List<String> ids = new ArrayList<>();
        metadata.find((Bson) query)
                .forEach((Consumer<Document>) document -> ids.add(document.getString("mdId")));
        List<String> databaseId =
                ids.stream()
                        .map(it -> getCurrentId(it, metadataManager))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

        final String hdfsuri = parser.get("namenode");
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        FileSystem.get(URI.create(hdfsuri), conf);
        Path hdfswritepath = new Path(parser.get("targetPath"));

        final AtomicInteger counter = new AtomicInteger(0);
        try (SequenceFile.Writer writer =
                SequenceFile.createWriter(
                        conf,
                        SequenceFile.Writer.file(hdfswritepath),
                        SequenceFile.Writer.keyClass(IntWritable.class),
                        SequenceFile.Writer.valueClass(Text.class))) {
            final IntWritable key = new IntWritable(counter.get());
            final Text value = new Text();
            databaseId.forEach(
                    id -> {
                        System.out.println("Reading :" + id);
                        MongoCollection<Document> collection = database.getCollection(id);
                        collection
                                .find()
                                .forEach(
                                        (Consumer<Document>)
                                                document -> {
                                                    key.set(counter.getAndIncrement());
                                                    value.set(document.getString("body"));

                                                    if (counter.get() % 10000 == 0) {
                                                        System.out.println(
                                                                "Added " + counter.get());
                                                    }
                                                    try {
                                                        writer.append(key, value);
                                                    } catch (IOException e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                });
                    });
        }
    }

    /**
     * Return the name of mongo collection giving an MdStore ID
     *
     * @param mdId The id of the MDStore
     * @param metadataManager The collection metadataManager on mongo which contains this
     *     information
     * @return
     */
    private static String getCurrentId(
            final String mdId, final MongoCollection<Document> metadataManager) {
        FindIterable<Document> result =
                metadataManager.find((Bson) QueryBuilder.start("mdId").is(mdId).get());
        final Document item = result.first();
        return item == null ? null : item.getString("currentId");
    }
}
