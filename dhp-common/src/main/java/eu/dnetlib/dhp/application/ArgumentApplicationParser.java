
package eu.dnetlib.dhp.application;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ArgumentApplicationParser implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(ArgumentApplicationParser.class);

	private final Options options = new Options();
	private final Map<String, String> objectMap = new HashMap<>();

	private final List<String> compressedValues = new ArrayList<>();

	public ArgumentApplicationParser(final String json_configuration) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final OptionsParameter[] configuration = mapper.readValue(json_configuration, OptionsParameter[].class);
		createOptionMap(configuration);
	}

	public ArgumentApplicationParser(final OptionsParameter[] configuration) {
		createOptionMap(configuration);
	}

	private void createOptionMap(final OptionsParameter[] configuration) {
		Arrays
			.stream(configuration)
			.map(
				conf -> {
					final Option o = new Option(conf.getParamName(), true, conf.getParamDescription());
					o.setLongOpt(conf.getParamLongName());
					o.setRequired(conf.isParamRequired());
					if (conf.isCompressed()) {
						compressedValues.add(conf.getParamLongName());
					}
					return o;
				})
			.forEach(options::addOption);
	}

	public static String decompressValue(final String abstractCompressed) {
		try {
			byte[] byteArray = Base64.decodeBase64(abstractCompressed.getBytes());
			GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(byteArray));
			final StringWriter stringWriter = new StringWriter();
			IOUtils.copy(gis, stringWriter);
			return stringWriter.toString();
		} catch (Throwable e) {
			log.error("Wrong value to decompress:" + abstractCompressed);
			throw new RuntimeException(e);
		}
	}

	public static String compressArgument(final String value) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out);
		gzip.write(value.getBytes());
		gzip.close();
		return java.util.Base64.getEncoder().encodeToString(out.toByteArray());
	}

	public void parseArgument(final String[] args) throws ParseException {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		Arrays
			.stream(cmd.getOptions())
			.forEach(
				it -> objectMap
					.put(
						it.getLongOpt(),
						compressedValues.contains(it.getLongOpt())
							? decompressValue(it.getValue())
							: it.getValue()));
	}

	public String get(final String key) {
		return objectMap.get(key);
	}

	public Map<String, String> getObjectMap() {
		return objectMap;
	}
}
