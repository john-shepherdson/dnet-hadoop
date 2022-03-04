
package eu.dnetlib.doiboost.crossref;

import static eu.dnetlib.dhp.common.collection.DecompressTarGz.doExtract;

import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.mortbay.log.Log;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class ExtractCrossrefRecords {
	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					ExtractCrossrefRecords.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/crossref_dump_reader/crossref_dump_reader.json")));
		parser.parseArgument(args);
		final String hdfsServerUri = parser.get("hdfsServerUri");
		final String workingPath = hdfsServerUri.concat(parser.get("workingPath"));
		final String outputPath = parser.get("outputPath");
		final String crossrefFileNameTarGz = parser.get("crossrefFileNameTarGz");

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", workingPath);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = FileSystem.get(URI.create(workingPath), conf);

		doExtract(fs, outputPath, workingPath.concat("/").concat(crossrefFileNameTarGz));

		Log.info("Crossref dump reading completed");

	}

}
