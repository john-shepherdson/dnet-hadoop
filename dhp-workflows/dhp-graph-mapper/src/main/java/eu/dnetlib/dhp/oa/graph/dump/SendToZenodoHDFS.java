
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.api.MissingConceptDoiException;
import eu.dnetlib.dhp.common.api.ZenodoAPIClient;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;

public class SendToZenodoHDFS implements Serializable {

	private static final Log log = LogFactory.getLog(SendToZenodoHDFS.class);

	public static void main(final String[] args) throws Exception, MissingConceptDoiException {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SendToZenodoHDFS.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/graph/dump/upload_zenodo.json")));

		parser.parseArgument(args);

		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("hdfsNameNode");
		final String access_token = parser.get("accessToken");
		final String connection_url = parser.get("connectionUrl");
		final String metadata = parser.get("metadata");
		final Boolean newDeposition = Boolean.valueOf(parser.get("newDeposition"));
		final String concept_rec_id = Optional
			.ofNullable(parser.get("conceptRecordId"))
			.orElse(null);
		final String communityMapPath = parser.get("communityMapPath");

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);

		CommunityMap communityMap = readCommunityMap(fileSystem, communityMapPath);

		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem
			.listFiles(
				new Path(hdfsPath), true);
		ZenodoAPIClient zenodoApiClient = new ZenodoAPIClient(connection_url, access_token);
		if (newDeposition) {
			zenodoApiClient.newDeposition();
		} else {
			if (concept_rec_id == null) {
				throw new MissingConceptDoiException("No concept record id has been provided");
			}
			zenodoApiClient.newVersion(concept_rec_id);
		}

		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();

			Path p = fileStatus.getPath();
			String p_string = p.toString();
			if (!p_string.endsWith("_SUCCESS")) {
				String tmp = p_string.substring(0, p_string.lastIndexOf("/"));
				String community = tmp.substring(tmp.lastIndexOf("/") + 1);
				log.info("Sending information for community: " + community);
				String community_name = communityMap.get(community).replace(" ", "_") + ".json.gz";

				FSDataInputStream inputStream = fileSystem.open(p);
				zenodoApiClient.uploadIS(inputStream, community_name, fileStatus.getLen());

			}

		}

		zenodoApiClient.sendMretadata(metadata);
		zenodoApiClient.publish();

	}

	public static CommunityMap readCommunityMap(FileSystem fileSystem, String communityMapPath) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(communityMapPath))));
		StringBuffer sb = new StringBuffer();
		try {
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
		} finally {
			br.close();

		}

		return new Gson().fromJson(sb.toString(), CommunityMap.class);
	}

}
