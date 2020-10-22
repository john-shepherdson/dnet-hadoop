
package eu.dnetlib.doiboost.orcidnodoi.oaf;

import static eu.dnetlib.doiboost.orcidnodoi.util.DumpToActionsUtility.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.*;

import eu.dnetlib.dhp.common.PacePerson;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.doiboost.orcidnodoi.util.DumpToActionsUtility;
import eu.dnetlib.doiboost.orcidnodoi.util.Pair;

/**
 * This class converts an orcid publication from json format to oaf
 */

public class PublicationToOaf implements Serializable {

	static Logger logger = LoggerFactory.getLogger(PublicationToOaf.class);

	public static final String ORCID = "ORCID";
	public final static String orcidPREFIX = "orcid_______";
	public static final String OPENAIRE_PREFIX = "openaire____";
	public static final String SEPARATOR = "::";

	private final LongAccumulator parsedPublications;
	private final LongAccumulator enrichedPublications;
	private final LongAccumulator errorsGeneric;
	private final LongAccumulator errorsInvalidTitle;
	private final LongAccumulator errorsNotFoundAuthors;
	private final LongAccumulator errorsInvalidType;

	public PublicationToOaf(
		LongAccumulator parsedPublications,
		LongAccumulator enrichedPublications,
		LongAccumulator errorsGeneric,
		LongAccumulator errorsInvalidTitle,
		LongAccumulator errorsNotFoundAuthors,
		LongAccumulator errorsInvalidType) {
		this.parsedPublications = parsedPublications;
		this.enrichedPublications = enrichedPublications;
		this.errorsGeneric = errorsGeneric;
		this.errorsInvalidTitle = errorsInvalidTitle;
		this.errorsNotFoundAuthors = errorsNotFoundAuthors;
		this.errorsInvalidType = errorsInvalidType;
	}

	public PublicationToOaf() {
		this.parsedPublications = null;
		this.enrichedPublications = null;
		this.errorsGeneric = null;
		this.errorsInvalidTitle = null;
		this.errorsNotFoundAuthors = null;
		this.errorsInvalidType = null;
	}

	private static Map<String, Pair<String, String>> datasources = new HashMap<String, Pair<String, String>>() {

		{
			put(ORCID.toLowerCase(), new Pair<>(ORCID, OPENAIRE_PREFIX + SEPARATOR + "orcid"));

		}
	};

	// json external id will be mapped to oaf:pid/@classid Map to oaf:pid/@classname
	private static Map<String, Pair<String, String>> externalIds = new HashMap<String, Pair<String, String>>() {

		{
			put("ark".toLowerCase(), new Pair<>("ark", "ark"));
			put("arxiv".toLowerCase(), new Pair<>("arxiv", "arXiv"));
			put("pmc".toLowerCase(), new Pair<>("pmc", "pmc"));
			put("pmid".toLowerCase(), new Pair<>("pmid", "pmid"));
			put("source-work-id".toLowerCase(), new Pair<>("orcidworkid", "orcidworkid"));
			put("urn".toLowerCase(), new Pair<>("urn", "urn"));
		}
	};

	static Map<String, Map<String, String>> typologiesMapping;

	static {
		try {
			final String tt = IOUtils
				.toString(
					PublicationToOaf.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/orcidnodoi/mappings/typologies.json"));
			typologiesMapping = new Gson().fromJson(tt, Map.class);
		} catch (Exception e) {
			throw new RuntimeException("loading typologies", e);
		}
	}

	public static final String PID_TYPES = "dnet:pid_types";

	public Oaf generatePublicationActionsFromJson(final String json) {
		try {
			if (parsedPublications != null) {
				parsedPublications.add(1);
			}
			JsonElement jElement = new JsonParser().parse(json);
			JsonObject jObject = jElement.getAsJsonObject();
			return generatePublicationActionsFromDump(jObject);
		} catch (Throwable t) {
			logger.error("creating publication: " + t.getMessage());
			if (errorsGeneric != null) {
				errorsGeneric.add(1);
			}
			return null;
		}
	}

	public Oaf generatePublicationActionsFromDump(final JsonObject rootElement) {

		if (!isValid(rootElement)) {
			return null;
		}

		Publication publication = new Publication();

		final DataInfo dataInfo = new DataInfo();
		dataInfo.setDeletedbyinference(false);
		dataInfo.setInferred(false);
		dataInfo.setTrust("0.9");
		dataInfo
			.setProvenanceaction(
				mapQualifier(
					"sysimport:actionset:orcidworks-no-doi",
					"sysimport:actionset:orcidworks-no-doi",
					"dnet:provenanceActions",
					"dnet:provenanceActions"));
		publication.setDataInfo(dataInfo);

		publication.setLastupdatetimestamp(new Date().getTime());

		publication.setDateofcollection("2019-10-22");
		publication.setDateoftransformation(DumpToActionsUtility.now_ISO8601());

		// Adding external ids
		externalIds
			.keySet()
			.stream()
			.forEach(jsonExtId -> {
				final String classid = externalIds.get(jsonExtId.toLowerCase()).getValue();
				final String classname = externalIds.get(jsonExtId.toLowerCase()).getKey();
				final String extId = getStringValue(rootElement, jsonExtId);
				if (StringUtils.isNotBlank(extId)) {
					publication
						.getExternalReference()
						.add(
							convertExtRef(extId, classid, classname, "dnet:pid_types", "dnet:pid_types"));
				}
			});

		// Adding source
		final String source = getStringValue(rootElement, "sourceName");
		if (StringUtils.isNotBlank(source)) {
			publication.setSource(Arrays.asList(mapStringField(source, null)));
		}

		// Adding titles
		final List<String> titles = createRepeatedField(rootElement, "titles");
		if (titles == null || titles.isEmpty()) {
			if (errorsInvalidTitle != null) {
				errorsInvalidTitle.add(1);
			}
			return null;
		}
		Qualifier q = mapQualifier("main title", "main title", "dnet:dataCite_title", "dnet:dataCite_title");
		publication
			.setTitle(
				titles
					.stream()
					.map(t -> {
						return mapStructuredProperty(t, q, null);
					})
					.collect(Collectors.toList()));
		// Adding identifier
		final String id = getStringValue(rootElement, "id");
		String sourceId = null;
		if (id != null) {
			publication.setOriginalId(Arrays.asList(id));
			sourceId = String.format("50|%s" + SEPARATOR + "%s", orcidPREFIX, DHPUtils.md5(id.toLowerCase()));
		} else {
			String mergedTitle = titles.stream().map(Object::toString).collect(Collectors.joining(","));
			sourceId = String.format("50|%s" + SEPARATOR + "%s", orcidPREFIX, DHPUtils.md5(mergedTitle.toLowerCase()));
		}
		publication.setId(sourceId);

		// Adding relevant date
		settingRelevantDate(rootElement, publication, "publication_date", "issued", true);

		// Adding collectedfrom
		publication.setCollectedfrom(Arrays.asList(createCollectedFrom()));

		// Adding type
		final String type = getStringValue(rootElement, "type");
		String cobjValue = "";
		if (StringUtils.isNotBlank(type)) {
			publication.setResourcetype(mapQualifier(type, type, "dnet:dataCite_resource", "dnet:dataCite_resource"));

			final String typeValue = typologiesMapping.get(type).get("value");
			cobjValue = typologiesMapping.get(type).get("cobj");
			final Instance instance = new Instance();

			// Adding hostedby
			instance.setHostedby(createHostedBy());

			// Adding url
			final List<String> urls = createRepeatedField(rootElement, "urls");
			if (urls != null && !urls.isEmpty()) {
				instance.setUrl(urls);
			} else {
				dataInfo.setInvisible(true);
			}

			final String pubDate = getPublicationDate(rootElement, "publicationDates");
			if (StringUtils.isNotBlank(pubDate)) {
				instance.setDateofacceptance(mapStringField(pubDate, null));
			}

			instance.setCollectedfrom(createCollectedFrom());

			// Adding accessright
			instance.setAccessright(mapQualifier("UNKNOWN", "UNKNOWN", "dnet:access_modes", "dnet:access_modes"));

			// Adding type
			instance
				.setInstancetype(
					mapQualifier(cobjValue, typeValue, "dnet:publication_resource", "dnet:publication_resource"));

			publication.setInstance(Arrays.asList(instance));
		} else {
			if (errorsInvalidType != null) {
				errorsInvalidType.add(1);
			}
			return null;
		}

		// Adding authors
		final List<Author> authors = createAuthors(rootElement);
		if (authors != null && authors.size() > 0) {
			publication.setAuthor(authors);
		} else {
			if (errorsNotFoundAuthors != null) {
				errorsNotFoundAuthors.add(1);
			}
			return null;
		}
		String classValue = getDefaultResulttype(cobjValue);
		publication
			.setResulttype(mapQualifier(classValue, classValue, "dnet:result_typologies", "dnet:result_typologies"));
		if (enrichedPublications != null) {
			enrichedPublications.add(1);
		}
		return publication;
	}

	public List<Author> createAuthors(final JsonObject root) {

		final String authorsJSONFieldName = "contributors";

		if (root.has(authorsJSONFieldName) && root.get(authorsJSONFieldName).isJsonArray()) {

			final List<Author> authors = new ArrayList<>();
			final JsonArray jsonAuthors = root.getAsJsonArray(authorsJSONFieldName);
			int firstCounter = 0;
			int defaultCounter = 0;
			int rank = 1;
			int currentRank = 0;

			for (final JsonElement item : jsonAuthors) {
				final JsonObject jsonAuthor = item.getAsJsonObject();
				final Author author = new Author();
				if (item.isJsonObject()) {
					final String creditname = getStringValue(jsonAuthor, "creditName");
					final String surname = getStringValue(jsonAuthor, "surname");
					final String name = getStringValue(jsonAuthor, "name");
					final String oid = getStringValue(jsonAuthor, "oid");
					final String seq = getStringValue(jsonAuthor, "sequence");
					if (StringUtils.isNotBlank(seq)) {
						if (seq.equals("first")) {
							firstCounter += 1;
							rank = firstCounter;

						} else if (seq.equals("additional")) {
							rank = currentRank + 1;
						} else {
							defaultCounter += 1;
							rank = defaultCounter;
						}
					}
					if (StringUtils.isNotBlank(oid)) {
						author.setPid(Arrays.asList(mapAuthorId(oid)));
						author.setFullname(name + " " + surname);
						if (StringUtils.isNotBlank(name)) {
							author.setName(name);
						}
						if (StringUtils.isNotBlank(surname)) {
							author.setSurname(surname);
						}
					} else {
						PacePerson p = new PacePerson(creditname, false);
						if (p.isAccurate()) {
							author.setName(p.getNormalisedFirstName());
							author.setSurname(p.getNormalisedSurname());
							author.setFullname(p.getNormalisedFullname());
						} else {
							author.setFullname(creditname);
						}
					}
				}
				author.setRank(rank);
				authors.add(author);
				currentRank = rank;
			}
			return authors;

		}
		return null;
	}

	private List<String> createRepeatedField(final JsonObject rootElement, final String fieldName) {
		if (!rootElement.has(fieldName)) {
			return null;
		}
		if (rootElement.has(fieldName) && rootElement.get(fieldName).isJsonNull()) {
			return null;
		}
		if (rootElement.get(fieldName).isJsonArray()) {
			if (!isValidJsonArray(rootElement, fieldName)) {
				return null;
			}
			return getArrayValues(rootElement, fieldName);
		} else {
			String field = getStringValue(rootElement, fieldName);
			return Arrays.asList(cleanField(field));
		}
	}

	private String cleanField(String value) {
		if (value != null && !value.isEmpty() && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
			value = value.substring(1, value.length() - 1);
		}
		return value;
	}

	private void settingRelevantDate(final JsonObject rootElement,
		final Publication publication,
		final String jsonKey,
		final String dictionaryKey,
		final boolean addToDateOfAcceptance) {

		final String pubDate = getPublicationDate(rootElement, "publication_date");
		if (StringUtils.isNotBlank(pubDate)) {
			if (addToDateOfAcceptance) {
				publication.setDateofacceptance(mapStringField(pubDate, null));
			}
			Qualifier q = mapQualifier(dictionaryKey, dictionaryKey, "dnet:dataCite_date", "dnet:dataCite_date");
			publication
				.setRelevantdate(
					Arrays
						.asList(pubDate)
						.stream()
						.map(r -> {
							return mapStructuredProperty(r, q, null);
						})
						.collect(Collectors.toList()));
		}
	}

	private String getPublicationDate(final JsonObject rootElement,
		final String jsonKey) {

		JsonObject pubDateJson = null;
		try {
			pubDateJson = rootElement.getAsJsonObject(jsonKey);
		} catch (Exception e) {
			return null;
		}
		if (pubDateJson == null) {
			return null;
		}
		final String year = getStringValue(pubDateJson, "year");
		final String month = getStringValue(pubDateJson, "month");
		final String day = getStringValue(pubDateJson, "day");

		if (StringUtils.isBlank(year)) {
			return null;
		}
		String pubDate = "".concat(year);
		if (StringUtils.isNotBlank(month)) {
			pubDate = pubDate.concat("-" + month);
			if (StringUtils.isNotBlank(day)) {
				pubDate = pubDate.concat("-" + day);
			} else {
				pubDate += "-01";
			}
		} else {
			pubDate += "-01-01";
		}
		if (isValidDate(pubDate)) {
			return pubDate;
		}
		return null;
	}

	protected boolean isValid(final JsonObject rootElement/* , final Reporter context */) {

		final String type = getStringValue(rootElement, "type");
		if (!typologiesMapping.containsKey(type)) {
			logger.error("unknowntype_" + type);
			if (errorsInvalidType != null) {
				errorsInvalidType.add(1);
			}
			return false;
		}

		if (!isValidJsonArray(rootElement, "titles")) {
			if (errorsInvalidTitle != null) {
				errorsInvalidTitle.add(1);
			}
			return false;
		}
		return true;
	}

	private boolean isValidJsonArray(final JsonObject rootElement, final String fieldName) {
		if (!rootElement.has(fieldName)) {
			return false;
		}
		final JsonElement jsonElement = rootElement.get(fieldName);
		if (jsonElement.isJsonNull()) {
			return false;
		}
		if (jsonElement.isJsonArray()) {
			final JsonArray jsonArray = jsonElement.getAsJsonArray();
			if (jsonArray.isJsonNull()) {
				return false;
			}
			if (jsonArray.get(0).isJsonNull()) {
				return false;
			}
		}
		return true;
	}

	private Qualifier mapQualifier(String classId, String className, String schemeId, String schemeName) {
		final Qualifier qualifier = new Qualifier();
		qualifier.setClassid(classId);
		qualifier.setClassname(className);
		qualifier.setSchemeid(schemeId);
		qualifier.setSchemename(schemeName);
		return qualifier;
	}

	private ExternalReference convertExtRef(String extId, String classId, String className, String schemeId,
		String schemeName) {
		ExternalReference ex = new ExternalReference();
		ex.setRefidentifier(extId);
		ex.setQualifier(mapQualifier(classId, className, schemeId, schemeName));
		return ex;
	}

	private StructuredProperty mapStructuredProperty(String value, Qualifier qualifier, DataInfo dataInfo) {
		if (value == null | StringUtils.isBlank(value)) {
			return null;
		}

		final StructuredProperty structuredProperty = new StructuredProperty();
		structuredProperty.setValue(value);
		structuredProperty.setQualifier(qualifier);
		structuredProperty.setDataInfo(dataInfo);
		return structuredProperty;
	}

	private Field<String> mapStringField(String value, DataInfo dataInfo) {
		if (value == null || StringUtils.isBlank(value)) {
			return null;
		}

		final Field<String> stringField = new Field<>();
		stringField.setValue(value);
		stringField.setDataInfo(dataInfo);
		return stringField;
	}

	private KeyValue createCollectedFrom() {
		KeyValue cf = new KeyValue();
		cf.setValue(ORCID);
		cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + "806360c771262b4d6770e7cdf04b5c5a");
		return cf;
	}

	private KeyValue createHostedBy() {
		KeyValue hb = new KeyValue();
		hb.setValue("Unknown Repository");
		hb.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + "55045bd2a65019fd8e6741a755395c8c");
		return hb;
	}

	private StructuredProperty mapAuthorId(String orcidId) {
		final StructuredProperty sp = new StructuredProperty();
		sp.setValue(orcidId);
		final Qualifier q = new Qualifier();
		q.setClassid(ORCID.toLowerCase());
		q.setClassname(ORCID.toLowerCase());
		q.setSchemeid(ModelConstants.DNET_PID_TYPES);
		q.setSchemename(ModelConstants.DNET_PID_TYPES);
		sp.setQualifier(q);
		return sp;
	}
}
