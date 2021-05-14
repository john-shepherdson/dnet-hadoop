
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.schema.common.ModelConstants.DATASET_DEFAULT_RESULTTYPE;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_ACCESS_MODES;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PID_TYPES;
import static eu.dnetlib.dhp.schema.common.ModelConstants.IS_PRODUCED_BY;
import static eu.dnetlib.dhp.schema.common.ModelConstants.NOT_AVAILABLE;
import static eu.dnetlib.dhp.schema.common.ModelConstants.ORP_DEFAULT_RESULTTYPE;
import static eu.dnetlib.dhp.schema.common.ModelConstants.OUTCOME;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PRODUCES;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PUBLICATION_DEFAULT_RESULTTYPE;
import static eu.dnetlib.dhp.schema.common.ModelConstants.REPOSITORY_PROVENANCE_ACTIONS;
import static eu.dnetlib.dhp.schema.common.ModelConstants.RESULT_PROJECT;
import static eu.dnetlib.dhp.schema.common.ModelConstants.SOFTWARE_DEFAULT_RESULTTYPE;
import static eu.dnetlib.dhp.schema.common.ModelConstants.UNKNOWN;
import static eu.dnetlib.dhp.schema.oaf.OafMapperUtils.createOpenaireId;
import static eu.dnetlib.dhp.schema.oaf.OafMapperUtils.dataInfo;
import static eu.dnetlib.dhp.schema.oaf.OafMapperUtils.field;
import static eu.dnetlib.dhp.schema.oaf.OafMapperUtils.journal;
import static eu.dnetlib.dhp.schema.oaf.OafMapperUtils.keyValue;
import static eu.dnetlib.dhp.schema.oaf.OafMapperUtils.listFields;
import static eu.dnetlib.dhp.schema.oaf.OafMapperUtils.oaiIProvenance;
import static eu.dnetlib.dhp.schema.oaf.OafMapperUtils.qualifier;
import static eu.dnetlib.dhp.schema.oaf.OafMapperUtils.structuredProperty;

import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentFactory;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.LicenseComparator;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.GeoLocation;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.OAIProvenance;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public abstract class AbstractMdRecordToOafMapper {

	protected final VocabularyGroup vocs;

	private final boolean invisible;

	protected static final String DATACITE_SCHEMA_KERNEL_4 = "http://datacite.org/schema/kernel-4";
	protected static final String DATACITE_SCHEMA_KERNEL_4_SLASH = "http://datacite.org/schema/kernel-4/";
	protected static final String DATACITE_SCHEMA_KERNEL_3 = "http://datacite.org/schema/kernel-3";
	protected static final String DATACITE_SCHEMA_KERNEL_3_SLASH = "http://datacite.org/schema/kernel-3/";
	protected static final Qualifier ORCID_PID_TYPE = qualifier(
		"ORCID", "Open Researcher and Contributor ID", DNET_PID_TYPES, DNET_PID_TYPES);
	protected static final Qualifier MAG_PID_TYPE = qualifier(
		"MAGIdentifier", "Microsoft Academic Graph Identifier", DNET_PID_TYPES, DNET_PID_TYPES);

	protected static final String DEFAULT_TRUST_FOR_VALIDATED_RELS = "0.999";

	protected static final Map<String, String> nsContext = new HashMap<>();

	static {
		nsContext.put("dr", "http://www.driver-repository.eu/namespace/dr");
		nsContext.put("dri", "http://www.driver-repository.eu/namespace/dri");
		nsContext.put("oaf", "http://namespace.openaire.eu/oaf");
		nsContext.put("oai", "http://www.openarchives.org/OAI/2.0/");
		nsContext.put("prov", "http://www.openarchives.org/OAI/2.0/provenance");
		nsContext.put("dc", "http://purl.org/dc/elements/1.1/");
		nsContext.put("datacite", DATACITE_SCHEMA_KERNEL_3);
	}

	protected static final Qualifier MAIN_TITLE_QUALIFIER = qualifier(
		"main title", "main title", "dnet:dataCite_title", "dnet:dataCite_title");

	protected AbstractMdRecordToOafMapper(final VocabularyGroup vocs, final boolean invisible) {
		this.vocs = vocs;
		this.invisible = invisible;
	}

	public List<Oaf> processMdRecord(final String xml) {
		try {
			DocumentFactory.getInstance().setXPathNamespaceURIs(nsContext);

			final Document doc = DocumentHelper
				.parseText(
					xml
						.replaceAll(DATACITE_SCHEMA_KERNEL_4, DATACITE_SCHEMA_KERNEL_3)
						.replaceAll(DATACITE_SCHEMA_KERNEL_4_SLASH, DATACITE_SCHEMA_KERNEL_3)
						.replaceAll(DATACITE_SCHEMA_KERNEL_3_SLASH, DATACITE_SCHEMA_KERNEL_3));

			final KeyValue collectedFrom = getProvenanceDatasource(
				doc, "//oaf:collectedFrom/@id", "//oaf:collectedFrom/@name");

			if (collectedFrom == null) {
				return null;
			}

			final KeyValue hostedBy = StringUtils.isBlank(doc.valueOf("//oaf:hostedBy/@id"))
				? collectedFrom
				: getProvenanceDatasource(doc, "//oaf:hostedBy/@id", "//oaf:hostedBy/@name");

			if (hostedBy == null) {
				return null;
			}

			final DataInfo info = prepareDataInfo(doc, invisible);
			final long lastUpdateTimestamp = new Date().getTime();

			final List<Instance> instances = prepareInstances(doc, info, collectedFrom, hostedBy);

			final String type = getResultType(doc, instances);

			return createOafs(doc, type, instances, collectedFrom, info, lastUpdateTimestamp);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected String getResultType(final Document doc, final List<Instance> instances) {
		final String type = doc.valueOf("//dr:CobjCategory/@type");

		if (StringUtils.isBlank(type) & vocs.vocabularyExists(ModelConstants.DNET_RESULT_TYPOLOGIES)) {
			final String instanceType = instances
				.stream()
				.map(i -> i.getInstancetype().getClassid())
				.findFirst()
				.map(s -> UNKNOWN.equalsIgnoreCase(s) ? "0000" : s)
				.orElse("0000"); // Unknown
			return Optional
				.ofNullable(vocs.getSynonymAsQualifier(ModelConstants.DNET_RESULT_TYPOLOGIES, instanceType))
				.map(q -> q.getClassid())
				.orElse("0000");
			/*
			 * .orElseThrow( () -> new IllegalArgumentException( String.format("'%s' not mapped in %s", instanceType,
			 * DNET_RESULT_TYPOLOGIES)));
			 */
		}

		return type;
	}

	private KeyValue getProvenanceDatasource(final Document doc, final String xpathId, final String xpathName) {
		final String dsId = doc.valueOf(xpathId);
		final String dsName = doc.valueOf(xpathName);

		if (StringUtils.isBlank(dsId) | StringUtils.isBlank(dsName)) {
			return null;
		}

		return keyValue(createOpenaireId(10, dsId, true), dsName);
	}

	protected List<Oaf> createOafs(
		final Document doc,
		final String type,
		final List<Instance> instances,
		final KeyValue collectedFrom,
		final DataInfo info,
		final long lastUpdateTimestamp) {

		final List<Oaf> oafs = new ArrayList<>();

		switch (type.toLowerCase()) {
			case "publication":
				final Publication p = new Publication();
				populateResultFields(p, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				p.setResulttype(PUBLICATION_DEFAULT_RESULTTYPE);
				p.setJournal(prepareJournal(doc, info));
				oafs.add(p);
				break;
			case "dataset":
				final Dataset d = new Dataset();
				populateResultFields(d, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				d.setResulttype(DATASET_DEFAULT_RESULTTYPE);
				d.setStoragedate(prepareDatasetStorageDate(doc, info));
				d.setDevice(prepareDatasetDevice(doc, info));
				d.setSize(prepareDatasetSize(doc, info));
				d.setVersion(prepareDatasetVersion(doc, info));
				d.setLastmetadataupdate(prepareDatasetLastMetadataUpdate(doc, info));
				d.setMetadataversionnumber(prepareDatasetMetadataVersionNumber(doc, info));
				d.setGeolocation(prepareDatasetGeoLocations(doc, info));
				oafs.add(d);
				break;
			case "software":
				final Software s = new Software();
				populateResultFields(s, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				s.setResulttype(SOFTWARE_DEFAULT_RESULTTYPE);
				s.setDocumentationUrl(prepareSoftwareDocumentationUrls(doc, info));
				s.setLicense(prepareSoftwareLicenses(doc, info));
				s.setCodeRepositoryUrl(prepareSoftwareCodeRepositoryUrl(doc, info));
				s.setProgrammingLanguage(prepareSoftwareProgrammingLanguage(doc, info));
				oafs.add(s);
				break;
			case "":
			case "otherresearchproducts":
			default:
				final OtherResearchProduct o = new OtherResearchProduct();
				populateResultFields(o, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				o.setResulttype(ORP_DEFAULT_RESULTTYPE);
				o.setContactperson(prepareOtherResearchProductContactPersons(doc, info));
				o.setContactgroup(prepareOtherResearchProductContactGroups(doc, info));
				o.setTool(prepareOtherResearchProductTools(doc, info));
				oafs.add(o);
				break;
		}

		if (!oafs.isEmpty()) {
			oafs.addAll(addProjectRels(doc, collectedFrom, info, lastUpdateTimestamp));
			oafs.addAll(addOtherResultRels(doc, collectedFrom, info, lastUpdateTimestamp));
		}

		return oafs;
	}

	private List<Oaf> addProjectRels(
		final Document doc,
		final KeyValue collectedFrom,
		final DataInfo info,
		final long lastUpdateTimestamp) {

		final List<Oaf> res = new ArrayList<>();

		final String docId = createOpenaireId(50, doc.valueOf("//dri:objIdentifier"), false);

		for (final Object o : doc.selectNodes("//oaf:projectid")) {

			final String originalId = ((Node) o).getText();

			final String validationdDate = ((Node) o).valueOf("@validationDate");

			if (StringUtils.isNotBlank(originalId)) {
				final String projectId = createOpenaireId(40, originalId, true);

				res
					.add(
						getRelationWithValidationDate(
							docId, projectId, RESULT_PROJECT, OUTCOME, IS_PRODUCED_BY, collectedFrom, info,
							lastUpdateTimestamp, validationdDate));
				res
					.add(
						getRelationWithValidationDate(
							projectId, docId, RESULT_PROJECT, OUTCOME, PRODUCES, collectedFrom, info,
							lastUpdateTimestamp, validationdDate));
			}
		}

		return res;
	}

	protected Relation getRelationWithValidationDate(final String source,
		final String target,
		final String relType,
		final String subRelType,
		final String relClass,
		final KeyValue collectedFrom,
		final DataInfo info,
		final long lastUpdateTimestamp,
		final String validationDate) {

		final Relation r = getRelation(
			source, target, relType, subRelType, relClass, collectedFrom, info, lastUpdateTimestamp);
		r.setValidated(StringUtils.isNotBlank(validationDate));
		r.setValidationDate(StringUtils.isNotBlank(validationDate) ? validationDate : null);

		if (StringUtils.isNotBlank(validationDate)) {
			r.setValidated(true);
			r.setValidationDate(validationDate);
			r.getDataInfo().setTrust(DEFAULT_TRUST_FOR_VALIDATED_RELS);
		} else {
			r.setValidated(false);
			r.setValidationDate(null);
		}

		return r;
	}

	protected Relation getRelation(final String source,
		final String target,
		final String relType,
		final String subRelType,
		final String relClass,
		final KeyValue collectedFrom,
		final DataInfo info,
		final long lastUpdateTimestamp) {
		final Relation rel = new Relation();
		rel.setRelType(relType);
		rel.setSubRelType(subRelType);
		rel.setRelClass(relClass);
		rel.setSource(source);
		rel.setTarget(target);
		rel.setCollectedfrom(Arrays.asList(collectedFrom));
		rel.setDataInfo(info);
		rel.setLastupdatetimestamp(lastUpdateTimestamp);
		return rel;
	}

	protected abstract List<Oaf> addOtherResultRels(
		final Document doc,
		final KeyValue collectedFrom,
		final DataInfo info,
		final long lastUpdateTimestamp);

	private void populateResultFields(
		final Result r,
		final Document doc,
		final List<Instance> instances,
		final KeyValue collectedFrom,
		final DataInfo info,
		final long lastUpdateTimestamp) {
		r.setDataInfo(info);
		r.setLastupdatetimestamp(lastUpdateTimestamp);
		r.setId(createOpenaireId(50, doc.valueOf("//dri:objIdentifier"), false));
		r.setOriginalId(findOriginalId(doc));
		r.setCollectedfrom(Arrays.asList(collectedFrom));
		r.setPid(prepareResultPids(doc, info));
		r.setDateofcollection(doc.valueOf("//dr:dateOfCollection|//dri:dateOfCollection"));
		r.setDateoftransformation(doc.valueOf("//dr:dateOfTransformation|//dri:dateOfTransformation"));
		r.setExtraInfo(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r.setOaiprovenance(prepareOAIprovenance(doc));
		r.setAuthor(prepareAuthors(doc, info));
		r.setLanguage(prepareLanguages(doc));
		r.setCountry(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r.setSubject(prepareSubjects(doc, info));
		r.setTitle(prepareTitles(doc, info));
		r.setRelevantdate(prepareRelevantDates(doc, info));
		r.setDescription(prepareDescriptions(doc, info));
		r.setDateofacceptance(prepareField(doc, "//oaf:dateAccepted", info));
		r.setPublisher(preparePublisher(doc, info));
		r.setEmbargoenddate(prepareField(doc, "//oaf:embargoenddate", info));
		r.setSource(prepareSources(doc, info));
		r.setFulltext(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r.setFormat(prepareFormats(doc, info));
		r.setContributor(prepareContributors(doc, info));
		r.setResourcetype(prepareResourceType(doc, info));
		r.setCoverage(prepareCoverages(doc, info));
		r.setContext(prepareContexts(doc, info));
		r.setExternalReference(new ArrayList<>()); // NOT PRESENT IN MDSTORES

		r.setInstance(instances);
		r.setBestaccessright(getBestAccessRights(instances));
	}

	protected abstract List<StructuredProperty> prepareResultPids(Document doc, DataInfo info);

	private List<Context> prepareContexts(final Document doc, final DataInfo info) {
		final List<Context> list = new ArrayList<>();
		for (final Object o : doc.selectNodes("//oaf:concept")) {
			final String cid = ((Node) o).valueOf("@id");
			if (StringUtils.isNotBlank(cid)) {
				final Context c = new Context();
				c.setId(cid);
				c.setDataInfo(Arrays.asList(info));
				list.add(c);
			}
		}
		return list;
	}

	protected abstract Qualifier prepareResourceType(Document doc, DataInfo info);

	protected abstract List<Instance> prepareInstances(
		Document doc,
		DataInfo info,
		KeyValue collectedfrom,
		KeyValue hostedby);

	protected abstract List<Field<String>> prepareSources(Document doc, DataInfo info);

	protected abstract List<StructuredProperty> prepareRelevantDates(Document doc, DataInfo info);

	protected abstract List<Field<String>> prepareCoverages(Document doc, DataInfo info);

	protected abstract List<Field<String>> prepareContributors(Document doc, DataInfo info);

	protected abstract List<Field<String>> prepareFormats(Document doc, DataInfo info);

	protected abstract Field<String> preparePublisher(Document doc, DataInfo info);

	protected abstract List<Field<String>> prepareDescriptions(Document doc, DataInfo info);

	protected abstract List<StructuredProperty> prepareTitles(Document doc, DataInfo info);

	protected abstract List<StructuredProperty> prepareSubjects(Document doc, DataInfo info);

	protected abstract Qualifier prepareLanguages(Document doc);

	protected abstract List<Author> prepareAuthors(Document doc, DataInfo info);

	protected abstract List<Field<String>> prepareOtherResearchProductTools(
		Document doc,
		DataInfo info);

	protected abstract List<Field<String>> prepareOtherResearchProductContactGroups(
		Document doc,
		DataInfo info);

	protected abstract List<Field<String>> prepareOtherResearchProductContactPersons(
		Document doc,
		DataInfo info);

	protected abstract Qualifier prepareSoftwareProgrammingLanguage(Document doc, DataInfo info);

	protected abstract Field<String> prepareSoftwareCodeRepositoryUrl(Document doc, DataInfo info);

	protected abstract List<StructuredProperty> prepareSoftwareLicenses(Document doc, DataInfo info);

	protected abstract List<Field<String>> prepareSoftwareDocumentationUrls(
		Document doc,
		DataInfo info);

	protected abstract List<GeoLocation> prepareDatasetGeoLocations(Document doc, DataInfo info);

	protected abstract Field<String> prepareDatasetMetadataVersionNumber(Document doc, DataInfo info);

	protected abstract Field<String> prepareDatasetLastMetadataUpdate(Document doc, DataInfo info);

	protected abstract Field<String> prepareDatasetVersion(Document doc, DataInfo info);

	protected abstract Field<String> prepareDatasetSize(Document doc, DataInfo info);

	protected abstract Field<String> prepareDatasetDevice(Document doc, DataInfo info);

	protected abstract Field<String> prepareDatasetStorageDate(Document doc, DataInfo info);

	public static Qualifier createBestAccessRights(final List<Instance> instanceList) {
		return getBestAccessRights(instanceList);
	}

	protected static Qualifier getBestAccessRights(final List<Instance> instanceList) {
		if (instanceList != null) {
			final Optional<Qualifier> min = instanceList
				.stream()
				.map(i -> i.getAccessright())
				.min(new LicenseComparator());

			final Qualifier rights = min.isPresent() ? min.get() : new Qualifier();

			if (StringUtils.isBlank(rights.getClassid())) {
				rights.setClassid(UNKNOWN);
			}
			if (StringUtils.isBlank(rights.getClassname())
				|| UNKNOWN.equalsIgnoreCase(rights.getClassname())) {
				rights.setClassname(NOT_AVAILABLE);
			}
			if (StringUtils.isBlank(rights.getSchemeid())) {
				rights.setSchemeid(DNET_ACCESS_MODES);
			}
			if (StringUtils.isBlank(rights.getSchemename())) {
				rights.setSchemename(DNET_ACCESS_MODES);
			}

			return rights;
		}
		return null;
	}

	private Journal prepareJournal(final Document doc, final DataInfo info) {
		final Node n = doc.selectSingleNode("//oaf:journal");
		if (n != null) {
			final String name = n.getText();
			final String issnPrinted = n.valueOf("@issn");
			final String issnOnline = n.valueOf("@eissn");
			final String issnLinking = n.valueOf("@lissn");
			final String ep = n.valueOf("@ep");
			final String iss = n.valueOf("@iss");
			final String sp = n.valueOf("@sp");
			final String vol = n.valueOf("@vol");
			final String edition = n.valueOf("@edition");
			if (StringUtils.isNotBlank(name)) {
				return journal(name, issnPrinted, issnOnline, issnLinking, ep, iss, sp, vol, edition, null, null, info);
			}
		}
		return null;
	}

	private List<String> findOriginalId(final Document doc) {
		final Node n = doc.selectSingleNode("//*[local-name()='provenance']/*[local-name()='originDescription']");
		if (n != null) {
			final String id = n.valueOf("./*[local-name()='identifier']");
			if (StringUtils.isNotBlank(id)) {
				return Lists.newArrayList(id);
			}
		}
		List<String> idList = doc
			.selectNodes(
				"normalize-space(//*[local-name()='header']/*[local-name()='identifier' or local-name()='recordIdentifier']/text())");
		Set<String> originalIds = Sets.newHashSet(idList);

		if (originalIds.isEmpty()) {
			throw new IllegalStateException("missing originalID on " + doc.asXML());
		}
		return Lists.newArrayList(originalIds);
	}

	protected Qualifier prepareQualifier(final Node node, final String xpath, final String schemeId) {
		return prepareQualifier(node.valueOf(xpath).trim(), schemeId);
	}

	protected Qualifier prepareQualifier(final String classId, final String schemeId) {
		return vocs.getTermAsQualifier(schemeId, classId);
	}

	protected List<StructuredProperty> prepareListStructProps(
		final Node node,
		final String xpath,
		final String xpathClassId,
		final String schemeId,
		final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();

		for (final Object o : node.selectNodes(xpath)) {
			final Node n = (Node) o;
			final String classId = n.valueOf(xpathClassId).trim();
			res.add(structuredProperty(n.getText(), prepareQualifier(classId, schemeId), info));
		}
		return res;
	}

	protected List<StructuredProperty> prepareListStructPropsWithValidQualifier(
		final Node node,
		final String xpath,
		final String xpathClassId,
		final String schemeId,
		final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();

		for (final Object o : node.selectNodes(xpath)) {
			final Node n = (Node) o;
			final String classId = n.valueOf(xpathClassId).trim();
			if (vocs.termExists(schemeId, classId)) {
				res.add(structuredProperty(n.getText(), vocs.getTermAsQualifier(schemeId, classId), info));
			}
		}
		return res;
	}

	protected List<StructuredProperty> prepareListStructProps(
		final Node node,
		final String xpath,
		final Qualifier qualifier,
		final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : node.selectNodes(xpath)) {
			final Node n = (Node) o;
			res.add(structuredProperty(n.getText(), qualifier, info));
		}
		return res;
	}

	protected List<StructuredProperty> prepareListStructProps(
		final Node node,
		final String xpath,
		final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : node.selectNodes(xpath)) {
			final Node n = (Node) o;
			res
				.add(
					structuredProperty(
						n.getText(), n.valueOf("@classid"), n.valueOf("@classname"), n.valueOf("@schemeid"),
						n.valueOf("@schemename"), info));
		}
		return res;
	}

	protected OAIProvenance prepareOAIprovenance(final Document doc) {
		final Node n = doc.selectSingleNode("//*[local-name()='provenance']/*[local-name()='originDescription']");

		if (n == null) {
			return null;
		}

		final String identifier = n.valueOf("./*[local-name()='identifier']");
		final String baseURL = n.valueOf("./*[local-name()='baseURL']");
		final String metadataNamespace = n.valueOf("./*[local-name()='metadataNamespace']");
		final boolean altered = n.valueOf("@altered").equalsIgnoreCase("true");
		final String datestamp = n.valueOf("./*[local-name()='datestamp']");
		final String harvestDate = n.valueOf("@harvestDate");

		return oaiIProvenance(identifier, baseURL, metadataNamespace, altered, datestamp, harvestDate);
	}

	protected DataInfo prepareDataInfo(final Document doc, final boolean invisible) {
		final Node n = doc.selectSingleNode("//oaf:datainfo");

		if (n == null) {
			return dataInfo(false, null, false, invisible, REPOSITORY_PROVENANCE_ACTIONS, "0.9");
		}

		final String paClassId = n.valueOf("./oaf:provenanceaction/@classid");
		final String paClassName = n.valueOf("./oaf:provenanceaction/@classname");
		final String paSchemeId = n.valueOf("./oaf:provenanceaction/@schemeid");
		final String paSchemeName = n.valueOf("./oaf:provenanceaction/@schemename");

		final boolean deletedbyinference = Boolean.parseBoolean(n.valueOf("./oaf:deletedbyinference"));
		final String inferenceprovenance = n.valueOf("./oaf:inferenceprovenance");
		final Boolean inferred = Boolean.parseBoolean(n.valueOf("./oaf:inferred"));
		final String trust = n.valueOf("./oaf:trust");

		return dataInfo(
			deletedbyinference, inferenceprovenance, inferred, invisible,
			qualifier(paClassId, paClassName, paSchemeId, paSchemeName), trust);
	}

	protected Field<String> prepareField(final Node node, final String xpath, final DataInfo info) {
		return field(node.valueOf(xpath), info);
	}

	protected List<Field<String>> prepareListFields(
		final Node node,
		final String xpath,
		final DataInfo info) {
		return listFields(info, prepareListString(node, xpath));
	}

	protected List<String> prepareListString(final Node node, final String xpath) {
		final List<String> res = new ArrayList<>();
		for (final Object o : node.selectNodes(xpath)) {
			final String s = ((Node) o).getText().trim();
			if (StringUtils.isNotBlank(s)) {
				res.add(s);
			}
		}
		return res;
	}
}
