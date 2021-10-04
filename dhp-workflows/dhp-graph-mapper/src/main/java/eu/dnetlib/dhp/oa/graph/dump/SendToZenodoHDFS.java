
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.api.MissingConceptDoiException;
import eu.dnetlib.dhp.common.api.ZenodoAPIClient;
import eu.dnetlib.dhp.oa.graph.dump.exceptions.NoAvailableEntityTypeException;

public class SendToZenodoHDFS implements Serializable {

	private static final String NEW = "new"; // to be used for a brand new deposition in zenodo
	private static final String VERSION = "version"; // to be used to upload a new version of a published deposition
	private static final String UPDATE = "update"; // to upload content to an open deposition not published

	public static void main(final String[] args) throws Exception, MissingConceptDoiException {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SendToZenodoHDFS.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/graph/dump/upload_zenodo.json")));

		parser.parseArgument(args);

		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("nameNode");
		final String access_token = parser.get("accessToken");
		final String connection_url = parser.get("connectionUrl");
		final String metadata = parser.get("metadata");
		final String depositionType = parser.get("depositionType");
		final String concept_rec_id = Optional
			.ofNullable(parser.get("conceptRecordId"))
			.orElse(null);
		final Boolean publish = Optional
			.ofNullable(parser.get("publish"))
			.map(Boolean::valueOf)
			.orElse(false);

		final String depositionId = Optional.ofNullable(parser.get("depositionId")).orElse(null);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);

		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem
			.listFiles(
				new Path(hdfsPath), true);
		ZenodoAPIClient zenodoApiClient = new ZenodoAPIClient(connection_url, access_token);
		switch (depositionType) {
			case NEW:
				zenodoApiClient.newDeposition();
				break;
			case VERSION:
				if (concept_rec_id == null) {
					throw new MissingConceptDoiException("No concept record id has been provided");
				}
				zenodoApiClient.newVersion(concept_rec_id);
				break;
			case UPDATE:
				if (depositionId == null) {
					throw new MissingConceptDoiException("No deposition id has been provided");
				}
				zenodoApiClient.uploadOpenDeposition(depositionId);
				break;
			default:
				throw new NoAvailableEntityTypeException();
		}

		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();

			Path p = fileStatus.getPath();
			String pString = p.toString();
			if (!pString.endsWith("_SUCCESS")) {
				String name = pString.substring(pString.lastIndexOf("/") + 1);

				FSDataInputStream inputStream = fileSystem.open(p);
				zenodoApiClient.uploadIS(inputStream, name, fileStatus.getLen());

			}

		}
		if (!metadata.equals("")) {
			zenodoApiClient.sendMretadata(metadata);
		}

		if (Boolean.TRUE.equals(publish)) {
			zenodoApiClient.publish();
		}
	}

}
