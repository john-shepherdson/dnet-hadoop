
package eu.dnetlib.dhp.common.collection;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class DecompressTarGz {

	public static void doExtract(String nameNode, String outputPath, String tarGzPath) throws IOException {
		Configuration conf = DHPUtils.getHadoopConfiguration(nameNode);
		Path p = new Path(tarGzPath);

		FSDataInputStream inputFileStream = p.getFileSystem(conf).open(p);
		try (TarArchiveInputStream tais = new TarArchiveInputStream(
			new GzipCompressorInputStream(inputFileStream))) {
			TarArchiveEntry entry = null;
			while ((entry = tais.getNextTarEntry()) != null) {
				if (!entry.isDirectory()) {
					Path outpath = new Path(outputPath.concat(entry.getName()).concat(".gz"));
					try (
						FSDataOutputStream out = outpath.getFileSystem(conf)
							.create(outpath);
						GZIPOutputStream gzipOs = new GZIPOutputStream(new BufferedOutputStream(out))) {

						IOUtils.copy(tais, gzipOs);

					}

				}
			}
		}
	}
}
