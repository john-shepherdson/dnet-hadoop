
package eu.dnetlib.dhp.monitoring;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class MongoLogsDumper implements Closeable {
	private static final Logger log = LoggerFactory.getLogger(MongoLogsDumper.class);
	private final MongoClient client;
	private final MongoCollection<Document> collection;

	private final FileSystem fileSystem;

	public MongoLogsDumper(final String host, final int port, final String database, final String collection,
		FileSystem fileSystem) {
		this.fileSystem = fileSystem;
		this.client = new MongoClient(host, port);
		this.collection = this.client.getDatabase(database).getCollection(collection);
	}

	public Iterator<AggregationLog> dumpLogs(final long startTime) {
		if (startTime == 0) {
			return this.collection.find().map(AggregationLog::fromMongoDocument).iterator();
		}
		return this.collection
			.find(new Document("log:date", new Document("$gt", startTime)))
			.map(AggregationLog::fromMongoDocument)
			.iterator();
	}

	public void writeLogs(final String path, final long strartTime) throws IOException {
		if (this.fileSystem == null)
			throw new IllegalArgumentException("FileSystem is null");
		ObjectMapper mapper = new ObjectMapper();
		try (SequenceFile.Writer file = SequenceFile
			.createWriter(
				this.fileSystem.getConf(),
				SequenceFile.Writer.file(new Path(path + "/log_dump.seq")),
				SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(Text.class))) {

			Text k = new Text();
			Text v = new Text();
			int[] counter = {
				0
			};
			this.dumpLogs(strartTime).forEachRemaining(l -> {
				try {
					v.clear();
					v.set(mapper.writeValueAsString(l));
					file.append(k, v);
					counter[0]++;
					if (counter[0] % 10000 == 0) {
						log.info("Dumped {} logs\n", counter[0]);
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		}
	}

	@Override
	public void close() throws IOException {
		this.client.close();
	}

	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							MongoLogsDumper.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/monitoring/dump_log_parameter.json"))));
		argumentParser.parseArgument(args);

		final String hdfsuri = argumentParser.get("namenode");
		log.info("hdfsURI is {}", hdfsuri);

		final String targetPath = argumentParser.get("targetPath");
		log.info("targetPath is {}", targetPath);

		final String mongoHost = argumentParser.get("mongoHost");
		log.info("mongoHost is {}", mongoHost);

		final String mongoDatabase = argumentParser.get("mongoDatabase");
		log.info("mongoDatabase is {}", mongoDatabase);

		final String mongoCollection = argumentParser.get("mongoCollection");
		log.info("mongoCollection is {}", mongoCollection);

		final String startTimeArgs = argumentParser.get("startTime");
		log.info("startTime is {}", startTimeArgs);

		final long startTime = startTimeArgs != null ? Long.parseLong(startTimeArgs) : 0L;

		final FileSystem fileSystem = FileSystem.get(getHadoopConfiguration(hdfsuri));

		new MongoLogsDumper(mongoHost, 27017, mongoDatabase, mongoCollection, fileSystem)
			.writeLogs(targetPath, startTime);

	}
}
