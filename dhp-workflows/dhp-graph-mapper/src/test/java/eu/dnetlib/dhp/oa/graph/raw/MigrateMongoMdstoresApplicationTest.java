
package eu.dnetlib.dhp.oa.graph.raw;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoDatabase;

import eu.dnetlib.dhp.common.MdstoreClient;
import io.fares.junit.mongodb.MongoExtension;
import io.fares.junit.mongodb.MongoForAllExtension;

@Disabled
public class MigrateMongoMdstoresApplicationTest {

	private static final Logger log = LoggerFactory.getLogger(MigrateMongoMdstoresApplicationTest.class);

	public static final String COLL_NAME = "9eed8a4d-bb41-47c3-987f-9d06aee0dec0::1453898911558";

	@RegisterExtension
	public static MongoForAllExtension mongo = MongoForAllExtension.defaultMongo();

	@BeforeAll
	public static void setUp() throws IOException {
		MongoDatabase db = mongo.getMongoClient().getDatabase(MongoExtension.UNIT_TEST_DB);

		db.getCollection(COLL_NAME).insertOne(Document.parse(read("mdstore_record.json")));
		db.getCollection("metadata").insertOne(Document.parse(read("mdstore_metadata.json")));
		db.getCollection("metadataManager").insertOne(Document.parse(read("mdstore_metadataManager.json")));
	}

	/*
	 * @Test public void test_MigrateMongoMdstoresApplication(@TempDir Path tmpPath) throws Exception { final String
	 * seqFile = "test_records.seq"; Path outputPath = tmpPath.resolve(seqFile); try (MigrateMongoMdstoresApplication
	 * app = new MigrateMongoMdstoresApplication( outputPath.toString(), mongo.getMongoClient(),
	 * MongoExtension.UNIT_TEST_DB)) { app.execute("oai_dc", "store", "native"); } Assertions .assertTrue( Files
	 * .list(tmpPath) .filter(f -> seqFile.contains(f.getFileName().toString())) .findFirst() .isPresent()); }
	 */

	private static String read(String filename) throws IOException {
		return IOUtils.toString(MigrateMongoMdstoresApplicationTest.class.getResourceAsStream(filename));
	}

}
