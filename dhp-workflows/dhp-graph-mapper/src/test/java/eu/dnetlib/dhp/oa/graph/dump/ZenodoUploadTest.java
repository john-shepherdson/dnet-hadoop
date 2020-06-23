
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import eu.dnetlib.dhp.oa.graph.dump.zenodo.Creator;
import eu.dnetlib.dhp.oa.graph.dump.zenodo.Metadata;
import eu.dnetlib.dhp.oa.graph.dump.zenodo.ZenodoModel;

public class ZenodoUploadTest {

	private static String workingDir;

	private final String URL_STRING = "https://sandbox.zenodo.org/api/deposit/depositions";
	private final String ACCESS_TOKEN = "5ImUj0VC1ICg4ifK5dc3AGzJhcfAB4osxrFlsr8WxHXxjaYgCE0hY8HZcDoe";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(eu.dnetlib.dhp.oa.graph.dump.UpdateProjectInfoTest.class.getSimpleName())
			.toString();
	}
//
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "localhost");
//
//		fileSystem = FileSystem.get(conf);
//
//		FSDataOutputStream fsDataOutputStream = fileSystem.create(new org.apache.hadoop.fs.Path(workingDir + "/ni"));
//
//		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
//
//
//		writer.write(ZenodoUploadTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/dump/zenodo/ni").toString());
//	}

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

		System.out.println("pr");

//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "localhost");
//
//		APIClient s = new APIClient(
		// "https://sandbox.zenodo.org/api/deposit/depositions");
//
//		s.connect();
//		s.upload(workingDir +"/ni", "Neuroinformatics", fs);

		APIClient client = new APIClient(URL_STRING,
			ACCESS_TOKEN);
		client.connect();

		// the second boolean parameter here sets the recursion to true
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs
			.listFiles(
				new Path(workingDir + "/zenodo"), true);
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			// do stuff with the file like ...

			// BufferedInputStream bis = new BufferedInputStream(fs.open( fileStatus.getPath()));
			String p_string = fileStatus.getPath().toString();

			int index = p_string.lastIndexOf("/");
			String community = p_string.substring(0, index);
			community = community.substring(community.lastIndexOf("/") + 1);
			String community_name = communityMap.get(community).replace(" ", "_");
			fs.copyToLocalFile(fileStatus.getPath(), new Path("/tmp/" + community_name));
			System.out.println(community);
//			System.out.println(client.upload(bis, community));

			File f = new File("/tmp/" + community_name);
			System.out.println(client.upload(f, community_name));

			if (f.exists()) {
				f.delete();
			}

		}

		ZenodoModel zenodo = new ZenodoModel();
		Metadata data = new Metadata();

		data.setTitle("Dump of OpenAIRE Communities related graph");
		data.setUpload_type("dataset");
		data.setDescription("this is a fake uploade done for testing purposes");
		Creator c = new Creator();
		c.setName("Miriam Baglioni");
		c.setAffiliation("CNR _ISTI");
		data.setCreators(Arrays.asList(c));
		zenodo.setMetadata(data);

		System.out.println(client.sendMretadata(new Gson().toJson(zenodo)));

		System.out.println(client.publish());

	}

	@Test
	public void serializeMetadata() {
		ZenodoModel zenodo = new ZenodoModel();
		Metadata data = new Metadata();

		data.setTitle("Dump of OpenAIRE Communities related graph");
		data.setUpload_type("dataset");
		data.setDescription("this is a fake uploade done for testing purposes");
		Creator c = new Creator();
		c.setName("Miriam Baglioni");
		c.setAffiliation("CNR _ISTI");
		data.setCreators(Arrays.asList(c));
		zenodo.setMetadata(data);

		System.out.println(new Gson().toJson(zenodo));

	}

	@Test
	public void testConnection() throws IOException {

		// InputStream is = getClass().getClassLoader().getResourceAsStream("eu/dnetlib/dhp/oa/graph/dump/zenodo/ni");
		APIClient s = new APIClient(
			"https://sandbox.zenodo.org/api/deposit/depositions",
			"5ImUj0VC1ICg4ifK5dc3AGzJhcfAB4osxrFlsr8WxHXxjaYgCE0hY8HZcDoe");

		Assertions.assertEquals(201, s.connect());

		s
			.upload(
				new File(getClass()
					.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/ni")
					.getPath()),
				"Neuroinformatics");

//		s.upload(getClass()
//				.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/dh-ch")
//				.getPath(), "DigitalHumanitiesandCulturalHeritage");
//
//		s.upload(getClass()
//				.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/egi")
//				.getPath(), "EGI");
//
//		s.upload(getClass()
//				.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/science-innovation-policy")
//				.getPath(), "ScienceandInnovationPolicyStudies");

		ZenodoModel zenodo = new ZenodoModel();
		Metadata data = new Metadata();

		data.setTitle("Dump of OpenAIRE Communities related graph");
		data.setUpload_type("dataset");
		data.setDescription("this is a fake uploade done for testing purposes");
		Creator c = new Creator();
		c.setName("Miriam Baglioni");
		c.setAffiliation("CNR _ISTI");
		data.setCreators(Arrays.asList(c));
		zenodo.setMetadata(data);

		s.sendMretadata(new Gson().toJson(zenodo));

		s.publish();

	}

}
