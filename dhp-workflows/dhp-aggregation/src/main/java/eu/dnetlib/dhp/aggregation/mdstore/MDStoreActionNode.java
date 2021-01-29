
package eu.dnetlib.dhp.aggregation.mdstore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.data.mdstore.manager.common.model.MDStoreVersion;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.worker.CollectorWorker;
import eu.dnetlib.dhp.common.rest.DNetRestClient;

public class MDStoreActionNode {
	private static final Logger log = LoggerFactory.getLogger(MDStoreActionNode.class);

	enum MDAction {
		NEW_VERSION, ROLLBACK, COMMIT, READ_LOCK, READ_UNLOCK

	}

	private static final ObjectMapper mapper = new ObjectMapper();

	public static String NEW_VERSION_URI = "%s/mdstore/%s/newVersion";

	public static final String COMMIT_VERSION_URL = "%s/version/%s/commit/%s";
	public static final String ROLLBACK_VERSION_URL = "%s/version/%s/abort";

	public static final String READ_LOCK_URL = "%s/mdstore/%s/startReading";
	public static final String READ_UNLOCK_URL = "%s/version/%s/endReading";

	private static final String MDSTOREVERSIONPARAM = "mdStoreVersion";
	private static final String MDSTOREREADLOCKPARAM = "mdStoreReadLockVersion";

	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					CollectorWorker.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/collection/mdstore_action_parameters.json")));
		argumentParser.parseArgument(args);

		final MDAction action = MDAction.valueOf(argumentParser.get("action"));
		log.info("Curren action is {}", action);

		final String mdStoreManagerURI = argumentParser.get("mdStoreManagerURI");
		log.info("mdStoreManagerURI is {}", mdStoreManagerURI);

		switch (action) {
			case NEW_VERSION: {
				final String mdStoreID = argumentParser.get("mdStoreID");
				if (StringUtils.isBlank(mdStoreID)) {
					throw new IllegalArgumentException("missing or empty argument mdStoreId");
				}
				final MDStoreVersion currentVersion = DNetRestClient
					.doGET(String.format(NEW_VERSION_URI, mdStoreManagerURI, mdStoreID), MDStoreVersion.class);
				populateOOZIEEnv(MDSTOREVERSIONPARAM, mapper.writeValueAsString(currentVersion));
				break;
			}
			case COMMIT: {

				final String hdfsuri = argumentParser.get("namenode");
				if (StringUtils.isBlank(hdfsuri)) {
					throw new IllegalArgumentException("missing or empty argument namenode");
				}
				final String mdStoreVersion_params = argumentParser.get("mdStoreVersion");
				final MDStoreVersion mdStoreVersion = mapper.readValue(mdStoreVersion_params, MDStoreVersion.class);

				if (StringUtils.isBlank(mdStoreVersion.getId())) {
					throw new IllegalArgumentException(
						"invalid MDStoreVersion value current is " + mdStoreVersion_params);
				}

				Configuration conf = new Configuration();
				// Set FileSystem URI
				conf.set("fs.defaultFS", hdfsuri);
				// Because of Maven
				conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
				conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

				System.setProperty("hadoop.home.dir", "/");
				// Get the filesystem - HDFS
				FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

				Path hdfstoreSizepath = new Path(mdStoreVersion.getHdfsPath() + "/size");

				FSDataInputStream inputStream = fs.open(hdfstoreSizepath);

				final Long mdStoreSize = Long.parseLong(IOUtils.toString(inputStream));

				inputStream.close();
				fs.create(hdfstoreSizepath);

				DNetRestClient
					.doGET(String.format(COMMIT_VERSION_URL, mdStoreManagerURI, mdStoreVersion.getId(), mdStoreSize));
				break;
			}
			case ROLLBACK: {
				final String mdStoreVersion_params = argumentParser.get("mdStoreVersion");
				final MDStoreVersion mdStoreVersion = mapper.readValue(mdStoreVersion_params, MDStoreVersion.class);

				if (StringUtils.isBlank(mdStoreVersion.getId())) {
					throw new IllegalArgumentException(
						"invalid MDStoreVersion value current is " + mdStoreVersion_params);
				}
				DNetRestClient.doGET(String.format(ROLLBACK_VERSION_URL, mdStoreManagerURI, mdStoreVersion.getId()));
				break;
			}

			case READ_LOCK: {
				final String mdStoreID = argumentParser.get("mdStoreID");
				if (StringUtils.isBlank(mdStoreID)) {
					throw new IllegalArgumentException("missing or empty argument mdStoreId");
				}
				final MDStoreVersion currentVersion = DNetRestClient
					.doGET(String.format(READ_LOCK_URL, mdStoreManagerURI, mdStoreID), MDStoreVersion.class);
				populateOOZIEEnv(MDSTOREREADLOCKPARAM, mapper.writeValueAsString(currentVersion));
				break;
			}
			case READ_UNLOCK: {
				final String mdStoreVersion_params = argumentParser.get("readMDStoreId");
				final MDStoreVersion mdStoreVersion = mapper.readValue(mdStoreVersion_params, MDStoreVersion.class);

				if (StringUtils.isBlank(mdStoreVersion.getId())) {
					throw new IllegalArgumentException(
						"invalid MDStoreVersion value current is " + mdStoreVersion_params);
				}
				DNetRestClient.doGET(String.format(READ_UNLOCK_URL, mdStoreManagerURI, mdStoreVersion.getId()));
				break;
			}

			default:
				throw new IllegalArgumentException("invalid action");
		}

	}

	public static void populateOOZIEEnv(final String paramName, String value) throws Exception {
		File file = new File(System.getProperty("oozie.action.output.properties"));
		Properties props = new Properties();

		props.setProperty(paramName, value);
		OutputStream os = new FileOutputStream(file);
		props.store(os, "");
		os.close();
	}
}
