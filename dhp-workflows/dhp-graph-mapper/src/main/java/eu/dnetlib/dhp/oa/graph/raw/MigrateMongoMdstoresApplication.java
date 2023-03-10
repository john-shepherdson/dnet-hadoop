
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.MDStoreInfo;
import eu.dnetlib.dhp.common.MdstoreClient;
import eu.dnetlib.dhp.oa.graph.raw.common.AbstractMigrationApplication;

public class MigrateMongoMdstoresApplication extends AbstractMigrationApplication implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(MigrateMongoMdstoresApplication.class);
	private final MdstoreClient mdstoreClient;

	private static List<MDStoreInfo> snapshotsMDStores(final MdstoreClient client,
		final String format,
		final String layout,
		final String interpretation) {
		return client.mdStoreWithTimestamp(format, layout, interpretation);
	}

	private static MDStoreInfo extractPath(final String path, final String basePath) {
		int res = path.indexOf(basePath);
		if (res > 0) {
			String[] split = path.substring(res).split("/");
			if (split.length > 2) {
				final String ts = split[split.length - 1];
				final String mdStore = split[split.length - 2];
				return new MDStoreInfo(mdStore, null, Long.parseLong(ts));
			}
		}
		return null;
	}

	private static Map<String, MDStoreInfo> hdfsMDStoreInfo(FileSystem fs, final String basePath) throws IOException {
		final Map<String, MDStoreInfo> hdfs_store = new HashMap<>();
		final Path p = new Path(basePath);
		final RemoteIterator<LocatedFileStatus> ls = fs.listFiles(p, true);
		while (ls.hasNext()) {

			String current = ls.next().getPath().toString();

			final MDStoreInfo info = extractPath(current, basePath);
			if (info != null) {
				hdfs_store.put(info.getMdstore(), info);
			}
		}
		return hdfs_store;
	}

	private static String createMDStoreDir(final String basePath, final String mdStoreId) {
		if (basePath.endsWith("/")) {
			return basePath + mdStoreId;
		} else {
			return String.format("%s/%s", basePath, mdStoreId);
		}
	}

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							MigrateMongoMdstoresApplication.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/oa/graph/migrate_mongo_mstores_parameters.json"))));
		parser.parseArgument(args);

		final String mongoBaseUrl = parser.get("mongoBaseUrl");
		final String mongoDb = parser.get("mongoDb");

		final String mdFormat = parser.get("mdFormat");
		final String mdLayout = parser.get("mdLayout");
		final String mdInterpretation = parser.get("mdInterpretation");

		final String hdfsPath = parser.get("hdfsPath");
		final String nameNode = parser.get("nameNode");

		final FileSystem fileSystem = FileSystem.get(getHadoopConfiguration(nameNode));

		final MdstoreClient mdstoreClient = new MdstoreClient(mongoBaseUrl, mongoDb);

		final List<MDStoreInfo> mongoMDStores = snapshotsMDStores(mdstoreClient, mdFormat, mdLayout, mdInterpretation);

		final Map<String, MDStoreInfo> hdfsMDStores = hdfsMDStoreInfo(fileSystem, hdfsPath);

		mongoMDStores
			.stream()
			.filter(currentMDStore -> currentMDStore.getLatestTimestamp() != null)
			.forEach(
				consumeMDStore(
					mdFormat, mdLayout, mdInterpretation, hdfsPath, fileSystem, mongoBaseUrl, mongoDb, hdfsMDStores));

		// TODO: DELETE MDStORE FOLDER NOT PRESENT IN MONGO

	}

	/**
	 * This method is responsible to sync only the stores that have been changed since last time
	 * @param mdFormat the MDStore's format
	 * @param mdLayout the MDStore'slayout
	 * @param mdInterpretation the MDStore's interpretation
	 * @param hdfsPath the basePath into hdfs where all MD-stores are stored
	 * @param fileSystem The Hadoop File system client
	 * @param hdfsMDStores A Map containing as Key the mdstore ID and as value the @{@link MDStoreInfo}
	 * @return
	 */
	private static Consumer<MDStoreInfo> consumeMDStore(String mdFormat, String mdLayout, String mdInterpretation,
		String hdfsPath, FileSystem fileSystem, final String mongoBaseUrl, final String mongoDb,
		Map<String, MDStoreInfo> hdfsMDStores) {
		return currentMDStore -> {
			// If the key is missing it means that the mdstore is not present in hdfs
			// that is the hdfs path basePath/MDSTOREID/timestamp is missing
			// So we have to synch it
			if (!hdfsMDStores.containsKey(currentMDStore.getMdstore())) {
				log.info("Adding store {}", currentMDStore.getMdstore());
				try {
					synchMDStoreIntoHDFS(
						mdFormat, mdLayout, mdInterpretation, hdfsPath, fileSystem, mongoBaseUrl, mongoDb,
						currentMDStore);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else {
				final MDStoreInfo current = hdfsMDStores.get(currentMDStore.getMdstore());
				// IF the key is present it means that in hdfs we have a path
				// basePath/MDSTOREID/timestamp but the timestamp on hdfs is older that the
				// new one in mongo so we have to synch the new mdstore and delete the old one
				if (currentMDStore.getLatestTimestamp() > current.getLatestTimestamp()) {
					log.info("Updating MDStore {}", currentMDStore.getMdstore());
					final String mdstoreDir = createMDStoreDir(hdfsPath, currentMDStore.getMdstore());
					final String rmPath = createMDStoreDir(mdstoreDir, current.getLatestTimestamp().toString());
					try {
						synchMDStoreIntoHDFS(
							mdFormat, mdLayout, mdInterpretation, hdfsPath, fileSystem, mongoBaseUrl, mongoDb,
							currentMDStore);
						log.info("deleting {}", rmPath);
						// DELETE THE OLD MDSTORE
						fileSystem.delete(new Path(rmPath), true);
					} catch (IOException e) {
						throw new RuntimeException("Unable to synch and remove path " + rmPath, e);
					}
				}
			}
		};
	}

	/**
	 *This method store into hdfs all the MONGO record of a single mdstore into the HDFS File
	 *
	 * @param mdFormat the MDStore's format
	 * @param mdLayout the MDStore'slayout
	 * @param mdInterpretation the MDStore's interpretation
	 * @param hdfsPath the basePath into hdfs where all MD-stores are stored
	 * @param fileSystem The Hadoop File system client
	 * @param currentMDStore The current Mongo MDStore ID
	 * @throws IOException
	 */
	private static void synchMDStoreIntoHDFS(String mdFormat, String mdLayout, String mdInterpretation, String hdfsPath,
		FileSystem fileSystem, final String mongoBaseUrl, final String mongoDb, MDStoreInfo currentMDStore)
		throws IOException {
		// FIRST CREATE the directory basePath/MDSTOREID
		final String mdstoreDir = createMDStoreDir(hdfsPath, currentMDStore.getMdstore());
		fileSystem.mkdirs(new Path(mdstoreDir));
		// Then synch all the records into basePath/MDSTOREID/timestamp
		final String currentIdDir = createMDStoreDir(mdstoreDir, currentMDStore.getLatestTimestamp().toString());
		try (MigrateMongoMdstoresApplication app = new MigrateMongoMdstoresApplication(mongoBaseUrl, mongoDb,
			currentIdDir)) {
			app.execute(currentMDStore.getCurrentId(), mdFormat, mdLayout, mdInterpretation);
		} catch (Exception e) {
			throw new RuntimeException(
				String
					.format("Error on sync mdstore with ID %s into path %s", currentMDStore.getMdstore(), currentIdDir),
				e);
		}
		log.info(String.format("Synchronized mdStore id : %s into path %s", currentMDStore.getMdstore(), currentIdDir));
	}

	public MigrateMongoMdstoresApplication(final String mongoBaseUrl, final String mongoDb, final String hdfsPath)
		throws Exception {
		super(hdfsPath);
		this.mdstoreClient = new MdstoreClient(mongoBaseUrl, mongoDb);
	}

	public void execute(final String currentColl, final String format, final String layout,
		final String interpretation) {
		for (final String xml : mdstoreClient.listRecords(currentColl)) {
			emit(xml, String.format("%s-%s-%s", format, layout, interpretation));
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		mdstoreClient.close();
	}
}
