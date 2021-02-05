
package eu.dnetlib.dhp.collection.worker.utils;

import static eu.dnetlib.dhp.aggregation.common.AggregationUtility.MAPPER;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.dnetlib.dhp.application.ApplicationUtils;

public class CollectorPluginReport extends LinkedHashMap<String, String> implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(CollectorPluginReport.class);

	@JsonIgnore
	private FileSystem fs;

	@JsonIgnore
	private Path path;

	@JsonIgnore
	private FSDataOutputStream fos;

	public static String SUCCESS = "success";

	public CollectorPluginReport() {
	}

	public CollectorPluginReport(FileSystem fs, Path path) throws IOException {
		this.fs = fs;
		this.path = path;

		this.fos = fs.create(path);
	}

	public Boolean isSuccess() {
		return Boolean.valueOf(get(SUCCESS));
	}

	public void setSuccess(Boolean success) {
		put(SUCCESS, String.valueOf(success));
	}

	@Override
	public void close() throws IOException {
		final String data = MAPPER.writeValueAsString(this);
		if (Objects.nonNull(fos)) {
			log.info("writing report {} to {}", data, path.toString());
			IOUtils.write(data, fos);
			ApplicationUtils.populateOOZIEEnv(this);
		}
	}
}
