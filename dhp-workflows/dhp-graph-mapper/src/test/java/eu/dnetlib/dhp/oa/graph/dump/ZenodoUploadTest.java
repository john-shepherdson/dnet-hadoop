
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.*;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import eu.dnetlib.dhp.oa.graph.dump.zenodo.*;
import eu.dnetlib.dhp.schema.dump.oaf.*;

public class ZenodoUploadTest {

	private static String workingDir;

	private final String URL_STRING = "https://sandbox.zenodo.org/api/deposit/depositions";
	private final String ACCESS_TOKEN = "";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(eu.dnetlib.dhp.oa.graph.dump.UpdateProjectInfoTest.class.getSimpleName())
			.toString();
	}


	@Test
	public void HDFSConnection() throws IOException {
		CommunityMap communityMap = new CommunityMap();
		communityMap.put("ni", "Neuroinformatics");
		communityMap.put("dh-ch", "Digital Humanities and Cultural Heritage");
		LocalFileSystem fs = FileSystem.getLocal(new Configuration());

		fs
			.copyFromLocalFile(
				false, new Path(getClass()
					.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/ni")
					.getPath()),
				new Path(workingDir + "/zenodo/ni/ni"));
		fs
			.copyFromLocalFile(
				false, new Path(getClass()
					.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/dh-ch")
					.getPath()),
				new Path(workingDir + "/zenodo/dh-ch/dh-ch"));


		APIClient client = new APIClient(URL_STRING,
			ACCESS_TOKEN);
		client.connect();

		// the second boolean parameter here sets the recursion to true
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs
			.listFiles(
				new Path(workingDir + "/zenodo"), true);
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();

			String p_string = fileStatus.getPath().toString();

			int index = p_string.lastIndexOf("/");
			String community = p_string.substring(0, index);
			community = community.substring(community.lastIndexOf("/") + 1);
			String community_name = communityMap.get(community).replace(" ", "_");
			fs.copyToLocalFile(fileStatus.getPath(), new Path("/tmp/" + community_name));
			System.out.println(community);

			File f = new File("/tmp/" + community_name);
			System.out.println(client.upload(f, community_name));

			if (f.exists()) {
				f.delete();
			}

		}


		String metadata = "{\"metadata\":{\"access_right\":\"open\",\"communities\":[{\"identifier\":\"openaire-research-graph\"}],\"creators\":[{\"affiliation\":\"CNR - ISTI\",\"name\":\"Manghi, Paolo\",\"orcid\":\"0000-0001-7291-3210\"},{\"affiliation\":\"CNR - ISTI\",\"name\":\"Atzori, Claudio\",\"orcid\":\"0000-0001-9613-6639\"},{\"affiliation\":\"CNR - ISTI\",\"name\":\"Bardi, Alessia\",\"orcid\":\"0000-0002-1112-1292\"},{\"affiliation\":\"ISTI - CNR\",\"name\":\"Baglioni, Miriam\",\"orcid\":\"0000-0002-2273-9004\"},{\"affiliation\":\"University of Bielefeld\",\"name\":\"Shirrwagen, Jochen\"},{\"affiliation\":\"Athena Research and Innovation Centre\",\"name\":\"Dimitropoulos, Harry\"},{\"affiliation\":\"CNR - ISTI\",\"name\":\"La Bruzzo, Sandro\",\"orcid\":\"0000-0003-2855-1245\"},{\"affiliation\":\"Athena Research and Innovation Centre\",\"name\":\"Foufoulas, Ioannis\"},{\"affiliation\":\"University of Bielefeld\",\"name\":\"Löhden, Aenne\"},{\"affiliation\":\"University of Bielefeld\",\"name\":\"Bäcker, Amelie\",\"orcid\":\"0000-0001-6015-2063\"},{\"affiliation\":\"CNR - ISTI\",\"name\":\"Mannocci, Andrea\",\"orcid\":\"0000-0002-5193-7851\"},{\"affiliation\":\"University of Warsaw\",\"name\":\"Horst, Marek\"},{\"affiliation\":\"University of Bielefeld\",\"name\":\"Czerniak, Andreas\",\"orcid\":\"0000-0003-3883-4169\"},{\"affiliation\":\"Athena Research and Innovation Centre\",\"name\":\"Kiatropoulou, Katerina\"},{\"affiliation\":\"Athena Research and Innovation Centre\",\"name\":\"Kokogiannaki, Argiro\",\"orcid\":\"0000-0002-3880-0244\"},{\"affiliation\":\"CNR - ISTI\",\"name\":\"De Bonis, Michele\"},{\"affiliation\":\"CNR - ISTI\",\"name\":\"Artini, Michele\"},{\"affiliation\":\"CNR - ISTI\",\"name\":\"Ottonello, Enrico\"},{\"affiliation\":\"Athena Research and Innovation Centre\",\"name\":\"Lempesis, Antonis\"},{\"affiliation\":\"CERN\",\"name\":\"Ioannidis, Alexandros\"},{\"affiliation\":\"University of Bielefeld\",\"name\":\"Summan, Friedrich\"}],\"description\":\"\\u003cp\\u003eThis dataset contains dumps of the OpenAIRE Research Graph containing metadata records relevant for the research communities and initiatives collaborating with OpenAIRE\\u003c/p\\u003e. \\u003cp\\u003eEach dataset is a zip containing a file with one json per line. Each json is compliant to the schema available at XXXX\\u003c/p\\u003e Note that the file that is offered is not a typical json file: each line contains a separate, self-contained json object. For more information please see http://jsonlines.org\",\"grants\":[{\"id\":\"777541\"},{\"id\":\"824091\"},{\"id\":\"824323\"}],\"keywords\":[\"Open Science\",\"Scholarly Communication\",\"Information Science\"],\"language\":\"eng\",\"license\":\"CC-BY-4.0\",\"title\":\"OpenAIRE Research Graph: Dumps for research communities and initiatives.\",\"upload_type\":\"dataset\",\"version\":\"1.0\"}}";

		System.out.println(client.sendMretadata(metadata));

		System.out.println(client.publish());

	}


}
