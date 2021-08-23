
package eu.dnetlib.dhp.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.JsonPath;

import net.minidev.json.JSONArray;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class DHPUtils {

	private static final Logger log = LoggerFactory.getLogger(DHPUtils.class);

	private DHPUtils() {
	}

	public static Seq<String> toSeq(List<String> list) {
		return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
	}

	public static String md5(final String s) {
		try {
			final MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(s.getBytes(StandardCharsets.UTF_8));
			return new String(Hex.encodeHex(md.digest()));
		} catch (final Exception e) {
			log.error("Error creating id from {}", s);
			return null;
		}
	}

	public static String generateIdentifier(final String originalId, final String nsPrefix) {
		return String.format("%s::%s", nsPrefix, DHPUtils.md5(originalId));
	}

	public static String getJPathString(final String jsonPath, final String json) {
		try {
			Object o = JsonPath.read(json, jsonPath);
			if (o instanceof String)
				return (String) o;
			if (o instanceof JSONArray && ((JSONArray) o).size() > 0)
				return (String) ((JSONArray) o).get(0);
			return o.toString();
		} catch (Exception e) {
			return "";
		}
	}

	public static final ObjectMapper MAPPER = new ObjectMapper();

	public static void writeHdfsFile(final Configuration conf, final String content, final String path)
		throws IOException {

		log.info("writing file {}, size {}", path, content.length());
		try (FileSystem fs = FileSystem.get(conf);
			BufferedOutputStream os = new BufferedOutputStream(fs.create(new Path(path)))) {
			os.write(content.getBytes(StandardCharsets.UTF_8));
			os.flush();
		}
	}

	public static String readHdfsFile(Configuration conf, String path) throws IOException {
		log.info("reading file {}", path);

		try (FileSystem fs = FileSystem.get(conf)) {
			final Path p = new Path(path);
			if (!fs.exists(p)) {
				throw new FileNotFoundException(path);
			}
			return IOUtils.toString(fs.open(p));
		}
	}

	public static <T> T readHdfsFileAs(Configuration conf, String path, Class<T> clazz) throws IOException {
		return MAPPER.readValue(readHdfsFile(conf, path), clazz);
	}

	public static <T> void saveDataset(final Dataset<T> mdstore, final String targetPath) {
		log.info("saving dataset in: {}", targetPath);
		mdstore
			.write()
			.mode(SaveMode.Overwrite)
			.format("parquet")
			.save(targetPath);
	}

	public static Configuration getHadoopConfiguration(String nameNode) {
		// ====== Init HDFS File System Object
		Configuration conf = new Configuration();
		// Set FileSystem URI
		conf.set("fs.defaultFS", nameNode);
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		System.setProperty("hadoop.home.dir", "/");
		return conf;
	}

	public static void populateOOZIEEnv(final Map<String, String> report) throws IOException {
		File file = new File(System.getProperty("oozie.action.output.properties"));
		Properties props = new Properties();
		report.forEach((k, v) -> props.setProperty(k, v));

		try (OutputStream os = new FileOutputStream(file)) {
			props.store(os, "");
		}
	}

	public static void populateOOZIEEnv(final String paramName, String value) throws IOException {
		Map<String, String> report = Maps.newHashMap();
		report.put(paramName, value);

		populateOOZIEEnv(report);
	}
}
