package eu.dnetlib.dhp.actionmanager;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class PromoteActionSetFromHDFSJobTest {
    private ClassLoader cl = getClass().getClassLoader();
    private Path workingDir;
    private Path inputActionSetDir;
    private Path outputDir;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("promote_action_set");
        inputActionSetDir = workingDir.resolve("input");
        outputDir = workingDir.resolve("output");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @Test
    public void shouldReadAtomicActionsFromHDFSAndWritePartitionedAsParquetFiles() throws Exception {
        // given
        // NOTE: test resource should contain atomic actions in a human readable form, probably as json files; here the
        // files should be converted to a serialized format and written out to workingDir/input
        // for current testing: actions from software export, given as sequence file are copied to workingDir/input/
        Path exportedActionSetDir = Paths.get(Objects.requireNonNull(cl.getResource("entities/entities_software")).getFile());
        Path inputDir = inputActionSetDir.resolve("entities_software");
        Files.createDirectories(inputDir);
        copyFiles(exportedActionSetDir, inputDir);
        PromoteActionSetFromHDFSJob.main(new String[]{
                "-mt", "local[*]",
                "-i", inputDir.toString(),
                "-o", outputDir.toString()
        });
    }

    private static void copyFiles(Path source, Path target) throws IOException {
        Files.list(source).forEach(f -> {
            try {
                Files.copy(f, target.resolve(f.getFileName()));
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }
}