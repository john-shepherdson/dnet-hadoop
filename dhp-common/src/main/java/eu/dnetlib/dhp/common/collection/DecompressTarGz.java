
package eu.dnetlib.dhp.common.collection;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DecompressTarGz {

	public static void doExtract(FileSystem fs, String outputPath, String tarGzPath) throws IOException {

		FSDataInputStream inputFileStream = fs.open(new Path(tarGzPath));
		try (TarArchiveInputStream tais = new TarArchiveInputStream(
			new GzipCompressorInputStream(inputFileStream))) {
			TarArchiveEntry entry = null;
			while ((entry = tais.getNextTarEntry()) != null) {
				if (!entry.isDirectory()) {
					try (
						FSDataOutputStream out = fs
							.create(new Path(outputPath.concat(entry.getName()).concat(".gz")));
						GZIPOutputStream gzipOs = new GZIPOutputStream(new BufferedOutputStream(out))) {

						IOUtils.copy(tais, gzipOs);

					}

				}
			}
		}
	}
}
