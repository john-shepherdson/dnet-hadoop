
package eu.dnetlib.doiboost.crossref;

import java.io.ByteArrayOutputStream;
import java.util.zip.Inflater;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class CrossrefImporter {

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					CrossrefImporter.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/import_from_es.json")));
		Logger logger = LoggerFactory.getLogger(CrossrefImporter.class);
		parser.parseArgument(args);

		final String hdfsuri = parser.get("namenode");
		logger.info("HDFS URI" + hdfsuri);
		Path hdfswritepath = new Path(parser.get("targetPath"));
		logger.info("TargetPath: " + hdfsuri);

		final Long timestamp = StringUtils.isNotBlank(parser.get("timestamp"))
			? Long.parseLong(parser.get("timestamp"))
			: -1;

		if (timestamp > 0)
			logger.info("Timestamp added " + timestamp);

		// ====== Init HDFS File System Object
		Configuration conf = new Configuration();
		// Set FileSystem URI
		conf.set("fs.defaultFS", hdfsuri);
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		ESClient client = timestamp > 0
			? new ESClient("ip-90-147-167-25.ct1.garrservices.it", "crossref", timestamp)
			: new ESClient("ip-90-147-167-25.ct1.garrservices.it", "crossref");

		try (SequenceFile.Writer writer = SequenceFile
			.createWriter(
				conf,
				SequenceFile.Writer.file(hdfswritepath),
				SequenceFile.Writer.keyClass(IntWritable.class),
				SequenceFile.Writer.valueClass(Text.class))) {

			int i = 0;
			long start = System.currentTimeMillis();
			long end = 0;
			final IntWritable key = new IntWritable(i);
			final Text value = new Text();
			while (client.hasNext()) {
				key.set(i++);
				value.set(client.next());
				writer.append(key, value);
				if (i % 1000000 == 0) {
					end = System.currentTimeMillis();
					final float time = (end - start) / 1000.0F;
					logger
						.info(
							String.format("Imported %d records last 100000 imported in %f seconds", i, time));
					start = System.currentTimeMillis();
				}
			}
		}
	}

	public static String decompressBlob(final String blob) {
		try {
			byte[] byteArray = Base64.decodeBase64(blob.getBytes());
			final Inflater decompresser = new Inflater();
			decompresser.setInput(byteArray);
			final ByteArrayOutputStream bos = new ByteArrayOutputStream(byteArray.length);
			byte[] buffer = new byte[8192];
			while (!decompresser.finished()) {
				int size = decompresser.inflate(buffer);
				bos.write(buffer, 0, size);
			}
			byte[] unzippeddata = bos.toByteArray();
			decompresser.end();
			return new String(unzippeddata);
		} catch (Throwable e) {
			throw new RuntimeException("Wrong record:" + blob, e);
		}
	}
}
