
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MakeTarTest {
	private static String workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(MakeTarTest.class.getSimpleName())
			.toString();
	}

	@Test
	public void testTar() throws IOException {
		LocalFileSystem fs = FileSystem.getLocal(new Configuration());

		fs
			.copyFromLocalFile(
				false, new Path(getClass()
					.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/ni")
					.getPath()),
				new Path(workingDir + "/zenodo/ni/part-00000"));

		fs
			.copyFromLocalFile(
				false, new Path(getClass()
					.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/egi")
					.getPath()),
				new Path(workingDir + "/zenodo/ni/part-00001"));

		fs
			.copyFromLocalFile(
				false, new Path(getClass()
					.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/dh-ch")
					.getPath()),
				new Path(workingDir + "/zenodo/dh-ch/part-00000"));
		fs
			.copyFromLocalFile(
				false, new Path(getClass()
					.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/science-innovation-policy")
					.getPath()),
				new Path(workingDir + "/zenodo/ni/part-00002"));

		String inputPath = workingDir + "/zenodo/";

		MakeTar.makeTArArchive(fs, inputPath, "/tmp/out");

	}
}
