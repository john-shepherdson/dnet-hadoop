
package eu.dnetlib.dhp.oa.graph.dump;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.Serializable;

public class SendToZenodoHDFS implements Serializable {

	private static final Log log = LogFactory.getLog(SendToZenodoHDFS.class);

	public static void main(final String[] args) throws Exception {
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
		final String isLookUpUrl = parser.get("isLookUpUrl");

		QueryInformationSystem qis = new QueryInformationSystem();
		qis.setIsLookUp(ISLookupClientFactory.getLookUpService(isLookUpUrl));
		CommunityMap communityMap = qis.getCommunityMap();

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);

		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem
			.listFiles(
				new Path(hdfsPath), true);
		APIClient apiClient = new APIClient(connection_url, access_token);
		apiClient.connect();
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();

			Path p = fileStatus.getPath();
			String p_string = p.toString();
			if (!p_string.endsWith("_SUCCESS")) {
				String tmp = p_string.substring(0, p_string.lastIndexOf("/"));
				String community = tmp.substring(tmp.lastIndexOf("/") + 1);
				log.info("Sending information for community: " + community);
				String community_name = communityMap.get(community).replace(" ", "_") + ".json.gz";
				//log.info("Copying information for community: " + community);
				//fileSystem.copyToLocalFile(p, new Path("/tmp/" + community_name));
				//File f = new File("/tmp/" + community_name);
				try {
					FSDataInputStream inputStream = fileSystem.open(p);
					apiClient.uploadIS(inputStream, community_name);

				} catch(Exception e){

				}
			}

		}

		apiClient.sendMretadata(metadata);
		apiClient.publish();

	}

}
