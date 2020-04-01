package eu.dnetlib.dhp.actionmanager.common;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class HdfsSupportTest {

    @Nested
    class Remove {

        @Test
        public void shouldThrowARuntimeExceptionOnError() {
            // when
            assertThrows(RuntimeException.class, () ->
                    HdfsSupport.remove(null, new Configuration()));
        }

        @Test
        public void shouldRemoveADirFromHDFS(@TempDir Path tempDir) {
            // when
            HdfsSupport.remove(tempDir.toString(), new Configuration());

            // then
            assertFalse(Files.exists(tempDir));
        }

        @Test
        public void shouldRemoveAFileFromHDFS(@TempDir Path tempDir) throws IOException {
            // given
            Path file = Files.createTempFile(tempDir, "p", "s");

            // when
            HdfsSupport.remove(file.toString(), new Configuration());

            // then
            assertFalse(Files.exists(file));
        }
    }

    @Nested
    class ListFiles {

        @Test
        public void shouldThrowARuntimeExceptionOnError() {
            // when
            assertThrows(RuntimeException.class, () ->
                    HdfsSupport.listFiles(null, new Configuration()));
        }

        @Test
        public void shouldListFilesLocatedInPath(@TempDir Path tempDir) throws IOException {
            Path subDir1 = Files.createTempDirectory(tempDir, "list_me");
            Path subDir2 = Files.createTempDirectory(tempDir, "list_me");

            // when
            List<String> paths = HdfsSupport.listFiles(tempDir.toString(), new Configuration());

            // then
            assertEquals(2, paths.size());
            List<String> expecteds = Arrays.stream(new String[]{subDir1.toString(), subDir2.toString()})
                    .sorted().collect(Collectors.toList());
            List<String> actuals = paths.stream().sorted().collect(Collectors.toList());
            assertTrue(actuals.get(0).contains(expecteds.get(0)));
            assertTrue(actuals.get(1).contains(expecteds.get(1)));
        }
    }
}
