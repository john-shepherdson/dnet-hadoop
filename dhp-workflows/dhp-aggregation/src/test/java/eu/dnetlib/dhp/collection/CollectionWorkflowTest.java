
package eu.dnetlib.dhp.collection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(MockitoExtension.class)
public class CollectionWorkflowTest {

	private static final Logger log = LoggerFactory.getLogger(CollectionWorkflowTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static Path workingDir;

	private static DistributedFileSystem fileSystem;

	// private static MiniDFSCluster hdfsCluster;

	private static ApiDescriptor api;
	private static String mdStoreVersion;

	private static final String encoding = "XML";
	private static final String dateOfCollection = System.currentTimeMillis() + "";
	private static final String xpath = "//*[local-name()='header']/*[local-name()='identifier']";
	private static String provenance;

	private static final String msgMgrUrl = "http://localhost:%s/mock/mvc/dhp/message";

	@BeforeAll
	protected static void beforeAll() throws Exception {
		provenance = IOUtils
			.toString(CollectionWorkflowTest.class.getResourceAsStream("/eu/dnetlib/dhp/collection/provenance.json"));

		workingDir = Files.createTempDirectory(CollectionWorkflowTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		/*
		 * Configuration conf = new Configuration(); conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
		 * workingDir.toString()); hdfsCluster = new MiniDFSCluster.Builder(conf).build(); fileSystem =
		 * hdfsCluster.getFileSystem(); api = OBJECT_MAPPER .readValue(
		 * IOUtils.toString(CollectionWorkflowTest.class.getResourceAsStream("apiDescriptor.json")),
		 * ApiDescriptor.class); mdStoreVersion = OBJECT_MAPPER
		 * .writeValueAsString(prepareVersion("/eu/dnetlib/dhp/collection/mdStoreVersion_1.json"));
		 */
	}

	@AfterAll
	protected static void tearDown() {
		/*
		 * hdfsCluster.shutdown(); FileUtil.fullyDelete(workingDir.toFile());
		 */

	}

	/**
	 <action name="CollectionWorker">
	 <java>
	 <main-class>eu.dnetlib.dhp.collection.worker.CollectorWorkerApplication</main-class>
	 <java-opts>${collection_java_xmx}</java-opts>
	 <arg>--apidescriptor</arg><arg>${apiDescription}</arg>
	 <arg>--namenode</arg><arg>${nameNode}</arg>
	 <arg>--workflowId</arg><arg>${workflowId}</arg>
	 <arg>--dnetMessageManagerURL</arg><arg>${dnetMessageManagerURL}</arg>
	 <arg>--mdStoreVersion</arg><arg>${wf:actionData('StartTransaction')['mdStoreVersion']}</arg>
	 <arg>--maxNumberOfRetry</arg><arg>${maxNumberOfRetry}</arg>
	 <arg>--requestDelay</arg><arg>${requestDelay}</arg>
	 <arg>--retryDelay</arg><arg>${retryDelay}</arg>
	 <arg>--connectTimeOut</arg><arg>${connectTimeOut}</arg>
	 <arg>--readTimeOut</arg><arg>${readTimeOut}</arg>
	 <capture-output/>
	 </java>
	 <ok to="CollectorReport"/>
	 <error to="CollectorReport"/>
	 </action>
	 */
	// @Test
	// @Order(1)
	public void testCollectorWorkerApplication() throws Exception {

		final HttpClientParams httpClientParams = new HttpClientParams();

		// String url = String.format(msgMgrUrl, wireMockServer.port());

		// new CollectorWorkerApplication(fileSystem).run(mdStoreVersion, httpClientParams, api, url, "1234");

	}

	public static MDStoreVersion prepareVersion(String filename) throws IOException {
		MDStoreVersion mdstore = OBJECT_MAPPER
			.readValue(IOUtils.toString(CollectionWorkflowTest.class.getResource(filename)), MDStoreVersion.class);
		mdstore.setHdfsPath(String.format(mdstore.getHdfsPath(), workingDir.toString()));
		return mdstore;
	}

}
