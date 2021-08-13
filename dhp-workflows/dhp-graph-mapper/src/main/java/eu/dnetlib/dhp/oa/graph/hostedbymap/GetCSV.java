
package eu.dnetlib.dhp.oa.graph.hostedbymap;

import java.io.*;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class GetCSV {

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GetCSV.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/graph/hostedbymap/download_csv_parameters.json")));

		parser.parseArgument(args);

		final String fileURL = parser.get("fileURL");
		final String hdfsPath = parser.get("workingPath");
		final String hdfsNameNode = parser.get("hdfsNameNode");
		final String classForName = parser.get("classForName");
		final String delimiter = Optional
			.ofNullable(parser.get("delimiter"))
			.orElse(null);
		final Boolean shouldReplace = Optional
			.ofNullable((parser.get("replace")))
			.map(Boolean::valueOf)
			.orElse(false);

		char del = ';';
		if (delimiter != null) {
			del = delimiter.charAt(0);
		}

		HttpConnector2 connector2 = new HttpConnector2();

		BufferedReader in = new BufferedReader(
			new InputStreamReader(connector2.getInputSourceAsStream(fileURL)));

		if (Boolean.TRUE.equals(shouldReplace)) {
			try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("/tmp/replaced.csv")))) {
				String line;
				while ((line = in.readLine()) != null) {
					writer.println(line.replace("\\\"", "\""));
				}
			}

			in.close();
			in = new BufferedReader(new FileReader("/tmp/replaced.csv"));
		}

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);

		eu.dnetlib.dhp.common.collection.GetCSV.getCsv(fileSystem, in, hdfsPath, classForName, del);

		in.close();
		if (Boolean.TRUE.equals(shouldReplace)) {
			File f = new File("/tmp/DOAJ.csv");
			f.delete();
		}

	}

}
