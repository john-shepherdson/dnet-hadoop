package eu.dnetlib.dhp.migration;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.ExtraInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.OAIProvenance;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OriginDescription;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.DHPUtils;

public class AbstractMigrationExecutor implements Closeable {

	private final AtomicInteger counter = new AtomicInteger(0);

	private final Text key = new Text();

	private final Text value = new Text();

	private final ObjectMapper objectMapper = new ObjectMapper();

	private final SequenceFile.Writer writer;

	private static final Log log = LogFactory.getLog(AbstractMigrationExecutor.class);

	public AbstractMigrationExecutor(final String hdfsPath, final String hdfsNameNode, final String hdfsUser) throws Exception {

		log.info(String.format("Creating SequenceFile Writer, hdfsPath=%s, nameNode=%s, user=%s", hdfsPath, hdfsNameNode, hdfsUser));

		this.writer = SequenceFile.createWriter(getConf(hdfsNameNode, hdfsUser), SequenceFile.Writer.file(new Path(hdfsPath)), SequenceFile.Writer
				.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class));
	}

	private Configuration getConf(final String hdfsNameNode, final String hdfsUser) throws IOException {
		final Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		System.setProperty("HADOOP_USER_NAME", hdfsUser);
		System.setProperty("hadoop.home.dir", "/");
		FileSystem.get(URI.create(hdfsNameNode), conf);
		return conf;
	}

	protected void emitOaf(final Oaf oaf) {
		try {
			key.set(counter.getAndIncrement() + ":" + oaf.getClass().getSimpleName().toLowerCase());
			value.set(objectMapper.writeValueAsString(oaf));
			writer.append(key, value);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		writer.hflush();
		writer.close();
	}

	public static KeyValue keyValue(final String k, final String v) {
		final KeyValue kv = new KeyValue();
		kv.setKey(k);
		kv.setValue(v);
		return kv;
	}

	public static List<KeyValue> listKeyValues(final String... s) {
		if (s.length % 2 > 0) { throw new RuntimeException("Invalid number of parameters (k,v,k,v,....)"); }

		final List<KeyValue> list = new ArrayList<>();
		for (int i = 0; i < s.length; i += 2) {
			list.add(keyValue(s[i], s[i + 1]));
		}
		return list;
	}

	public static <T> Field<T> field(final T value, final DataInfo info) {
		if (value == null || StringUtils.isBlank(value.toString())) { return null; }

		final Field<T> field = new Field<>();
		field.setValue(value);
		field.setDataInfo(info);
		return field;
	}

	public static List<Field<String>> listFields(final DataInfo info, final String... values) {
		return Arrays.stream(values).map(v -> field(v, info)).filter(Objects::nonNull).collect(Collectors.toList());
	}

	public static List<Field<String>> listFields(final DataInfo info, final List<String> values) {
		return values.stream().map(v -> field(v, info)).filter(Objects::nonNull).collect(Collectors.toList());
	}

	public static Qualifier qualifier(final String classid, final String classname, final String schemeid, final String schemename) {
		final Qualifier q = new Qualifier();
		q.setClassid(classid);
		q.setClassname(classname);
		q.setSchemeid(schemeid);
		q.setSchemename(schemename);
		return q;
	}

	public static StructuredProperty structuredProperty(final String value,
			final String classid,
			final String classname,
			final String schemeid,
			final String schemename,
			final DataInfo dataInfo) {

		return structuredProperty(value, qualifier(classid, classname, schemeid, schemename), dataInfo);
	}

	public static StructuredProperty structuredProperty(final String value, final Qualifier qualifier, final DataInfo dataInfo) {
		if (value == null) { return null; }
		final StructuredProperty sp = new StructuredProperty();
		sp.setValue(value);
		sp.setQualifier(qualifier);
		sp.setDataInfo(dataInfo);
		return sp;
	}

	public static ExtraInfo extraInfo(final String name, final String value, final String typology, final String provenance, final String trust) {
		final ExtraInfo info = new ExtraInfo();
		info.setName(name);
		info.setValue(value);
		info.setTypology(typology);
		info.setProvenance(provenance);
		info.setTrust(trust);
		return info;
	}

	public static OAIProvenance oaiIProvenance(final String identifier,
			final String baseURL,
			final String metadataNamespace,
			final Boolean altered,
			final String datestamp,
			final String harvestDate) {

		final OriginDescription desc = new OriginDescription();
		desc.setIdentifier(identifier);
		desc.setBaseURL(baseURL);
		desc.setMetadataNamespace(metadataNamespace);
		desc.setAltered(altered);
		desc.setDatestamp(datestamp);
		desc.setHarvestDate(harvestDate);

		final OAIProvenance p = new OAIProvenance();
		p.setOriginDescription(desc);

		return p;
	}

	public static Journal journal(final String name,
			final String issnPrinted,
			final String issnOnline,
			final String issnLinking,
			final String ep,
			final String iss,
			final String sp,
			final String vol,
			final String edition,
			final String conferenceplace,
			final String conferencedate,
			final DataInfo dataInfo) {

		if (StringUtils.isNotBlank(name) || StringUtils.isNotBlank(issnPrinted) || StringUtils.isNotBlank(issnOnline) || StringUtils.isNotBlank(issnLinking)) {
			final Journal j = new Journal();
			j.setName(name);
			j.setIssnPrinted(issnPrinted);
			j.setIssnOnline(issnOnline);
			j.setIssnLinking(issnLinking);
			j.setEp(ep);
			j.setIss(iss);
			j.setSp(sp);
			j.setVol(vol);
			j.setEdition(edition);
			j.setConferenceplace(conferenceplace);
			j.setConferencedate(conferencedate);
			j.setDataInfo(dataInfo);
			return j;
		} else {
			return null;
		}
	}

	public static DataInfo dataInfo(final Boolean deletedbyinference,
			final String inferenceprovenance,
			final Boolean inferred,
			final Boolean invisible,
			final Qualifier provenanceaction,
			final String trust) {
		final DataInfo d = new DataInfo();
		d.setDeletedbyinference(deletedbyinference);
		d.setInferenceprovenance(inferenceprovenance);
		d.setInferred(inferred);
		d.setInvisible(invisible);
		d.setProvenanceaction(provenanceaction);
		d.setTrust(trust);
		return d;
	}

	public static String createOpenaireId(final int prefix, final String originalId) {
		final String nsPrefix = StringUtils.substringBefore(originalId, "::");
		final String rest = StringUtils.substringAfter(originalId, "::");
		return String.format("%s|%s::%s", prefix, nsPrefix, DHPUtils.md5(rest));
	}

	public static String createOpenaireId(final String type, final String originalId) {
		switch (type) {
		case "datasource":
			return createOpenaireId(10, originalId);
		case "organization":
			return createOpenaireId(20, originalId);
		case "person":
			return createOpenaireId(30, originalId);
		case "project":
			return createOpenaireId(40, originalId);
		default:
			return createOpenaireId(50, originalId);
		}
	}

	public static String asString(final Object o) {
		return o == null ? "" : o.toString();
	}
}
