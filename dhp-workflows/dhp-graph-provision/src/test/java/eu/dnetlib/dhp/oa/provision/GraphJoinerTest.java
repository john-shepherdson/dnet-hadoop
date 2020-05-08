
package eu.dnetlib.dhp.oa.provision;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;

public class GraphJoinerTest {

	private final ClassLoader cl = getClass().getClassLoader();
	private Path workingDir;
	private Path inputDir;
	private Path outputDir;

	@BeforeEach
	public void before() throws IOException {
		workingDir = Files.createTempDirectory("promote_action_set");
		inputDir = workingDir.resolve("input");
		outputDir = workingDir.resolve("output");
	}

	private static void copyFiles(Path source, Path target) throws IOException {
		Files
			.list(source)
			.forEach(
				f -> {
					try {
						if (Files.isDirectory(f)) {
							Path subTarget = Files.createDirectories(target.resolve(f.getFileName()));
							copyFiles(f, subTarget);
						} else {
							Files.copy(f, target.resolve(f.getFileName()));
						}
					} catch (IOException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				});
	}
}
