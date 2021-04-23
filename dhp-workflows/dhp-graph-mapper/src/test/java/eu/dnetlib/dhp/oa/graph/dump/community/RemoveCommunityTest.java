
package eu.dnetlib.dhp.oa.graph.dump.community;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.graph.dump.MakeTar;
import eu.dnetlib.dhp.oa.graph.dump.MakeTarTest;

public class RemoveCommunityTest {

	private static String workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(MakeTarTest.class.getSimpleName())
			.toString();
	}

	@Test
	public void testRemove() throws Exception {
		LocalFileSystem fs = FileSystem.getLocal(new Configuration());

		fs
			.copyFromLocalFile(
				false, new Path(getClass()
					.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
					.getPath()),
				new Path(workingDir + "/communityMap"));

		String path = workingDir + "/communityMap";

		RemoveCommunities.main(new String[] {
			"-nameNode", "local",
			"-path", path,
			"-communityId", "beopen"
		}

		);

		CommunityMap cm = new ObjectMapper()
			.readValue(new FileInputStream(workingDir + "/communityMap"), CommunityMap.class);

		Assertions.assertEquals(1, cm.size());
		Assertions.assertTrue(cm.containsKey("beopen"));

	}
}
