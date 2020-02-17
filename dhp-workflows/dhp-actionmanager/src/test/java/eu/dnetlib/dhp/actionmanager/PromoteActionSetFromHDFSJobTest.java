package eu.dnetlib.dhp.actionmanager;

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
    private Path inputDir;
    private Path outputDir;

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("promote_action_set");
        inputDir = workingDir.resolve("input");
        outputDir = workingDir.resolve("output");
    }

//    @After
//    public void after() throws IOException {
//        FileUtils.deleteDirectory(workingDir.toFile());
//    }

    @Test
    public void shouldReadAtomicActionsFromHDFSAndWritePartitionedAsParquetFiles() throws Exception {
        // given
        // NOTE: test resource should contain atomic actions in a human readable form, probably as json files; here the
        // files should be converted to a serialized format and written out to workingDir/input
        // for current testing: actions from iis export, given as sequence file are copied to workingDir/input/

        //graph
        Path inputGraphDir = inputDir.resolve("graph");
        Files.createDirectories(inputGraphDir);
        copyFiles(Paths.get(Objects.requireNonNull(cl.getResource("graph")).getFile()), inputGraphDir);

        //actions
        Path inputActionsDir = inputDir.resolve("actions");
        Files.createDirectories(inputActionsDir);

        Path inputEntitiesPatentDir = inputActionsDir.resolve("entities_patent");
        Files.createDirectories(inputEntitiesPatentDir);
        copyFiles(Paths.get(Objects.requireNonNull(cl.getResource("actions/entities_patent")).getFile()), inputEntitiesPatentDir);

        Path inputEntitiesSoftwareDir = inputActionsDir.resolve("entities_software");
        Files.createDirectories(inputEntitiesSoftwareDir);
        copyFiles(Paths.get(Objects.requireNonNull(cl.getResource("actions/entities_software")).getFile()), inputEntitiesSoftwareDir);

        String inputActionSetPaths = String.join(",",   inputEntitiesSoftwareDir.toString()); //inputEntitiesPatentDir.toString(),

        PromoteActionSetFromHDFSJob.main(new String[]{
                "-master", "local[*]",
                "-inputGraphPath", inputGraphDir.toString(),
                "-inputActionSetPaths", inputActionSetPaths,
                "-outputGraphPath", outputDir.toString()
        });
    }

    private static void copyFiles(Path source, Path target) throws IOException {
        Files.list(source).forEach(f -> {
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