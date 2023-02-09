
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PID_TYPES;
import static eu.dnetlib.dhp.schema.common.ModelConstants.OUTCOME;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PRODUCES;
import static eu.dnetlib.dhp.schema.common.ModelConstants.REPOSITORY_PROVENANCE_ACTIONS;
import static eu.dnetlib.dhp.schema.common.ModelConstants.RESULT_PROJECT;
import static eu.dnetlib.dhp.schema.common.ModelConstants.UNKNOWN;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.*;
import static eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory.*;

import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.oaf.Entity;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.dom4j.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

public abstract class AbstractMdRecordToOafMapper {

	protected final VocabularyGroup vocs;

	private final boolean invisible;

	private final boolean shouldHashId;

	private final boolean forceOriginalId;

	protected static final String DATACITE_SCHEMA_KERNEL_4 = "http://datacite.org/schema/kernel-4";
	protected static final String DATACITE_SCHEMA_KERNEL_4_SLASH = "http://datacite.org/schema/kernel-4/";
	protected static final String DATACITE_SCHEMA_KERNEL_3 = "http://datacite.org/schema/kernel-3";
	protected static final String DATACITE_SCHEMA_KERNEL_3_SLASH = "http://datacite.org/schema/kernel-3/";
	protected static final Qualifier ORCID_PID_TYPE = qualifier(
		ModelConstants.ORCID_PENDING,
		ModelConstants.ORCID_CLASSNAME,
		DNET_PID_TYPES);
	protected static final Qualifier MAG_PID_TYPE = qualifier(
		"MAGIdentifier", "Microsoft Academic Graph Identifier", DNET_PID_TYPES);

	protected static final String DEFAULT_TRUST_FOR_VALIDATED_RELS = "0.999";

	protected static final Map<String, String> nsContext = new HashMap<>();

	private static final Logger log = LoggerFactory.getLogger(AbstractMdRecordToOafMapper.class);

	static {
		nsContext.put("dr", "http://www.driver-repository.eu/namespace/dr");
		nsContext.put("dri", "http://www.driver-repository.eu/namespace/dri");
		nsContext.put("oaf", "http://namespace.openaire.eu/oaf");
		nsContext.put("oai", "http://www.openarchives.org/OAI/2.0/");
		nsContext.put("prov", "http://www.openarchives.org/OAI/2.0/provenance");
		nsContext.put("dc", "http://purl.org/dc/elements/1.1/");
		nsContext.put("datacite", DATACITE_SCHEMA_KERNEL_3);
	}

	// lowercase pidTypes as keys, normal casing for the values
	protected static final Map<String, String> pidTypeWithAuthority = new HashMap<>();

	static {
		IdentifierFactory.PID_AUTHORITY
			.keySet()
			.stream()
			.forEach(entry -> pidTypeWithAuthority.put(entry.toString().toLowerCase(), entry.toString()));

	}

	protected AbstractMdRecordToOafMapper(final VocabularyGroup vocs, final boolean invisible,
		final boolean shouldHashId, final boolean forceOriginalId) {
		this.vocs = vocs;
		this.invisible = invisible;
		this.shouldHashId = shouldHashId;
		this.forceOriginalId = forceOriginalId;
	}

	protected AbstractMdRecordToOafMapper(final VocabularyGroup vocs, final boolean invisible,
		final boolean shouldHashId) {
		this.vocs = vocs;
		this.invisible = invisible;
		this.shouldHashId = shouldHashId;
		this.forceOriginalId = false;
	}

	public List<Oaf> processMdRecord(final String xml) {

		DocumentFactory.getInstance().setXPathNamespaceURIs(nsContext);
		try {
			final Document doc = DocumentHelper
				.parseText(
					xml
						.replaceAll(DATACITE_SCHEMA_KERNEL_4, DATACITE_SCHEMA_KERNEL_3)
						.replaceAll(DATACITE_SCHEMA_KERNEL_4_SLASH, DATACITE_SCHEMA_KERNEL_3)
						.replaceAll(DATACITE_SCHEMA_KERNEL_3_SLASH, DATACITE_SCHEMA_KERNEL_3));

			final KeyValue collectedFrom = getProvenanceDatasource(
				doc, "//oaf:collectedFrom/@id", "//oaf:collectedFrom/@name");

			if (collectedFrom == null) {
				return Lists.newArrayList();
			}

			final KeyValue hostedBy = StringUtils.isBlank(doc.valueOf("//oaf:hostedBy/@id"))
				? collectedFrom
				: getProvenanceDatasource(doc, "//oaf:hostedBy/@id", "//oaf:hostedBy/@name");

			if (hostedBy == null) {
				return Lists.newArrayList();
			}

			final EntityDataInfo info = prepareDataInfo(doc, invisible);
			final long lastUpdateTimestamp = new Date().getTime();

			final List<Instance> instances = prepareInstances(doc, info, collectedFrom, hostedBy);

			final String type = getResultType(doc, instances);

			return createOafs(doc, type, instances, collectedFrom, info, lastUpdateTimestamp);
		} catch (DocumentException e) {
			log.error("Error with record:\n" + xml);
			return Lists.newArrayList();
		}
	}

	protected String getResultType(final Document doc, final List<Instance> instances) {
		final String type = doc.valueOf("//dr:CobjCategory/@type");

		if (StringUtils.isBlank(type) && vocs.vocabularyExists(ModelConstants.DNET_RESULT_TYPOLOGIES)) {
			final String instanceType = instances
				.stream()
				.map(i -> i.getInstancetype().getClassid())
				.findFirst()
				.filter(s -> !UNKNOWN.equalsIgnoreCase(s))
				.orElse("0000"); // Unknown
			return Optional
				.ofNullable(vocs.getSynonymAsQualifier(ModelConstants.DNET_RESULT_TYPOLOGIES, instanceType))
				.map(Qualifier::getClassid)
				.orElse("0000");
		}

		return type;
	}

	private KeyValue getProvenanceDatasource(final Document doc, final String xpathId, final String xpathName) {
		final String dsId = doc.valueOf(xpathId);
		final String dsName = doc.valueOf(xpathName);

		if (StringUtils.isBlank(dsId) || StringUtils.isBlank(dsName)) {
			return null;
		}

		return keyValue(createOpenaireId(10, dsId, true), dsName);
	}

	protected List<Oaf> createOafs(
		final Document doc,
		final String type,
		final List<Instance> instances,
		final KeyValue collectedFrom,
		final EntityDataInfo info,
		final long lastUpdateTimestamp) {

		final Entity entity = createEntity(doc, type, instances, collectedFrom, info, lastUpdateTimestamp);

		final Set<String> originalId = Sets.newHashSet(entity.getOriginalId());
		originalId.add(entity.getId());
		entity.setOriginalId(Lists.newArrayList(originalId));

		if (!forceOriginalId) {
			final String id = IdentifierFactory.createIdentifier(entity, shouldHashId);
			if (!id.equals(entity.getId())) {
				entity.setId(id);
			}
		}

		final List<Oaf> oafs = Lists.newArrayList(entity);

		if (!oafs.isEmpty()) {
			Set<Oaf> rels = Sets.newHashSet();

			rels.addAll(addProjectRels(doc, entity));
			rels.addAll(addOtherResultRels(doc, entity));
			rels.addAll(addRelations(doc, entity));

			oafs.addAll(rels);
		}

		return oafs;
	}

	private Entity createEntity(final Document doc,
		final String type,
		final List<Instance> instances,
		final KeyValue collectedFrom,
		final EntityDataInfo info,
		final long lastUpdateTimestamp) {
		switch (type.toLowerCase()) {
			case "publication":
				final Publication p = new Publication();
				populateResultFields(p, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				p.setJournal(prepareJournal(doc));
				return p;
			case "dataset":
				final Dataset d = new Dataset();
				populateResultFields(d, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				d.setStoragedate(prepareDatasetStorageDate(doc));
				d.setDevice(prepareDatasetDevice(doc));
				d.setSize(prepareDatasetSize(doc));
				d.setVersion(prepareDatasetVersion(doc));
				d.setLastmetadataupdate(prepareDatasetLastMetadataUpdate(doc));
				d.setMetadataversionnumber(prepareDatasetMetadataVersionNumber(doc));
				d.setGeolocation(prepareDatasetGeoLocations(doc));
				return d;
			case "software":
				final Software s = new Software();
				populateResultFields(s, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				s.setDocumentationUrl(prepareSoftwareDocumentationUrls(doc));
				s.setCodeRepositoryUrl(prepareSoftwareCodeRepositoryUrl(doc));
				s.setProgrammingLanguage(prepareSoftwareProgrammingLanguage(doc));
				return s;
			case "":
			case "otherresearchproducts":
			default:
				final OtherResearchProduct o = new OtherResearchProduct();
				populateResultFields(o, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				o.setContactperson(prepareOtherResearchProductContactPersons(doc));
				o.setContactgroup(prepareOtherResearchProductContactGroups(doc));
				o.setTool(prepareOtherResearchProductTools(doc));
				return o;
		}
	}

	private List<Oaf> addProjectRels(
		final Document doc,
		final Entity entity) {

		final List<Oaf> res = new ArrayList<>();

		final String docId = entity.getId();

		for (final Object o : doc.selectNodes("//oaf:projectid")) {

			final String originalId = ((Node) o).getText();

			final String validationdDate = ((Node) o).valueOf("@validationDate");

			if (StringUtils.isNotBlank(originalId)) {
				final String projectId = createOpenaireId(40, originalId, true);

				res
					.add(
						OafMapperUtils
							.getRelation(projectId, docId, RESULT_PROJECT, OUTCOME, PRODUCES, entity, validationdDate));
			}
		}

		return res;
	}

	private List<Oaf> addRelations(Document doc, Entity entity) {

		final List<Oaf> rels = Lists.newArrayList();

		for (Object o : doc.selectNodes("//oaf:relation")) {
			Element element = (Element) o;

			final String target = StringUtils.trim(element.getText());
			final String relType = element.attributeValue("relType");
			final String subRelType = element.attributeValue("subRelType");
			final String relClass = element.attributeValue("relClass");

			if (StringUtils.isNotBlank(target) && StringUtils.isNotBlank(relType) && StringUtils.isNotBlank(subRelType)
				&& StringUtils.isNotBlank(relClass)) {

				final String validationdDate = ((Node) o).valueOf("@validationDate");

				if (StringUtils.isNotBlank(target)) {
					final String targetType = element.attributeValue("targetType");
					if (StringUtils.isNotBlank(targetType)) {
						final String targetId = createOpenaireId(targetType, target, true);
						rels
							.add(
								OafMapperUtils
									.getRelation(
										entity.getId(), targetId, relType, subRelType, relClass, entity,
										validationdDate));
					}
				}
			}
		}
		return rels;
	}

	protected abstract List<Oaf> addOtherResultRels(
		final Document doc,
		final Entity entity);

	private void populateResultFields(
		final Result r,
		final Document doc,
		final List<Instance> instances,
		final KeyValue collectedFrom,
		final EntityDataInfo info,
		final long lastUpdateTimestamp) {
		r.setDataInfo(info);
		r.setLastupdatetimestamp(lastUpdateTimestamp);
		r.setId(createOpenaireId(50, doc.valueOf("//dri:objIdentifier"), false));
		r.setOriginalId(findOriginalId(doc));
		r.setCollectedfrom(Arrays.asList(collectedFrom));
		r.setPid(IdentifierFactory.getPids(prepareResultPids(doc, info), collectedFrom));
		r.setDateofcollection(doc.valueOf("//dr:dateOfCollection/text()|//dri:dateOfCollection/text()"));
		r.setDateoftransformation(doc.valueOf("//dr:dateOfTransformation/text()|//dri:dateOfTransformation/text()"));
		r.setExtraInfo(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r.setOaiprovenance(prepareOAIprovenance(doc));
		r.setAuthor(prepareAuthors(doc, info));
		r.setLanguage(prepareLanguages(doc));
		r.setCountry(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r.setSubject(prepareSubjects(doc, info));
		r.setTitle(prepareTitles(doc));
		r.setRelevantdate(prepareRelevantDates(doc));
		r.setDescription(prepareDescriptions(doc));
		r.setDateofacceptance(doc.valueOf( "//oaf:dateAccepted"));
		r.setPublisher(preparePublisher(doc));
		r.setEmbargoenddate(doc.valueOf("//oaf:embargoenddate"));
		r.setSource(prepareSources(doc));
		r.setFulltext(prepareListString(doc, "//oaf:fulltext"));
		r.setFormat(prepareFormats(doc));
		r.setContributor(prepareContributors(doc));
		r.setResourcetype(prepareResourceType(doc));
		r.setCoverage(prepareCoverages(doc));
		r.setContext(prepareContexts(doc, info));
		r.setExternalReference(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r
			.setProcessingchargeamount(doc.valueOf("//oaf:processingchargeamount"));
		r
			.setProcessingchargecurrency(doc.valueOf("//oaf:processingchargeamount/@currency"));

		r.setInstance(instances);
		r.setBestaccessright(OafMapperUtils.createBestAccessRights(instances));
		r.setEoscifguidelines(prepareEOSCIfGuidelines(doc, info));
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

	private List<EoscIfGuidelines> prepareEOSCIfGuidelines(Document doc, DataInfo info) {
		final Set<EoscIfGuidelines> set = Sets.newHashSet();
		for (final Object o : doc.selectNodes("//oaf:eoscifguidelines")) {
			final String code = ((Node) o).valueOf("@code");
			final String label = ((Node) o).valueOf("@label");
			final String url = ((Node) o).valueOf("@url");
			final String semrel = ((Node) o).valueOf("@semanticrelation");
			if (StringUtils.isNotBlank(code)) {
				final EoscIfGuidelines eig = new EoscIfGuidelines();
				eig.setCode(code);
				eig.setLabel(label);
				eig.setUrl(url);
				eig.setSemanticRelation(semrel);
				set.add(eig);
			}
		}
		return Lists.newArrayList(set);
	}

	protected abstract Qualifier prepareResourceType(Document doc);

	protected abstract List<Instance> prepareInstances(
		Document doc,
		DataInfo info,
		KeyValue collectedfrom,
		KeyValue hostedby);

	protected abstract List<String> prepareSources(Document doc);

	protected abstract List<StructuredProperty> prepareRelevantDates(Document doc);

	protected abstract List<String> prepareCoverages(Document doc);

	protected abstract List<String> prepareContributors(Document doc);

	protected abstract List<String> prepareFormats(Document doc);

	protected abstract Publisher preparePublisher(Document doc);

	protected abstract List<String> prepareDescriptions(Document doc);

	protected abstract List<StructuredProperty> prepareTitles(Document doc);

	protected abstract List<Subject> prepareSubjects(Document doc, DataInfo info);

	protected abstract Qualifier prepareLanguages(Document doc);

	protected abstract List<Author> prepareAuthors(Document doc, DataInfo info);

	protected abstract List<String> prepareOtherResearchProductTools(Document doc);

	protected abstract List<String> prepareOtherResearchProductContactGroups(Document doc);

	protected abstract List<String> prepareOtherResearchProductContactPersons(Document doc);

	protected abstract Qualifier prepareSoftwareProgrammingLanguage(Document doc);

	protected abstract String prepareSoftwareCodeRepositoryUrl(Document doc);

	protected abstract List<String> prepareSoftwareDocumentationUrls(Document doc);

	protected abstract List<GeoLocation> prepareDatasetGeoLocations(Document doc);

	protected abstract String prepareDatasetMetadataVersionNumber(Document doc);

	protected abstract String prepareDatasetLastMetadataUpdate(Document doc);

	protected abstract String prepareDatasetVersion(Document doc);

	protected abstract String prepareDatasetSize(Document doc);

	protected abstract String prepareDatasetDevice(Document doc);

	protected abstract String prepareDatasetStorageDate(Document doc);

	private Journal prepareJournal(final Document doc) {
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
				return journal(name, issnPrinted, issnOnline, issnLinking, ep, iss, sp, vol, edition, null, null);
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
		final List<String> idList = doc
			.selectNodes(
				"normalize-space(//*[local-name()='header']/*[local-name()='identifier' or local-name()='recordIdentifier']/text())");
		final Set<String> originalIds = Sets.newHashSet(idList);

		if (originalIds.isEmpty()) {
			throw new IllegalStateException("missing originalID on " + doc.asXML());
		}
		return Lists.newArrayList(originalIds);
	}

	protected AccessRight prepareAccessRight(final Node node, final String xpath, final String schemeId) {
		final Qualifier qualifier = prepareQualifier(node.valueOf(xpath).trim(), schemeId);
		final AccessRight accessRight = new AccessRight();
		accessRight.setClassid(qualifier.getClassid());
		accessRight.setClassname(qualifier.getClassname());
		accessRight.setSchemeid(qualifier.getSchemeid());

		// TODO set the OAStatus

		return accessRight;
	}

	protected Qualifier prepareQualifier(final Node node, final String xpath, final String schemeId) {
		return prepareQualifier(node.valueOf(xpath).trim(), schemeId);
	}

	protected Qualifier prepareQualifier(final String classId, final String schemeId) {
		return vocs.getTermAsQualifier(schemeId, classId);
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
				res.add(structuredProperty(n.getText(), vocs.getTermAsQualifier(schemeId, classId)));
			}
		}
		return res;
	}

	protected List<StructuredProperty> prepareListStructProps(
		final Node node,
		final String xpath,
		final Qualifier qualifier) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : node.selectNodes(xpath)) {
			final Node n = (Node) o;
			res.add(structuredProperty(n.getText(), qualifier));
		}
		return res;
	}

	protected List<Subject> prepareSubjectList(
		final Node node,
		final String xpath,
		final DataInfo info) {
		final List<Subject> res = new ArrayList<>();
		for (final Object o : node.selectNodes(xpath)) {
			final Node n = (Node) o;
			Qualifier qualifier = qualifier(n.valueOf("@classid"), n.valueOf("@classname"), n.valueOf("@schemeid"));
			res
				.add(
					subject(n.getText(), qualifier, info));
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

	protected EntityDataInfo prepareDataInfo(final Document doc, final boolean invisible) {
		final Node n = doc.selectSingleNode("//oaf:datainfo");

		if (n == null) {
			return dataInfo(false, false, 0.9f, null, false, REPOSITORY_PROVENANCE_ACTIONS);
		}

		final String paClassId = n.valueOf("./oaf:provenanceaction/@classid");
		final String paClassName = n.valueOf("./oaf:provenanceaction/@classname");
		final String paSchemeId = n.valueOf("./oaf:provenanceaction/@schemeid");

		final boolean deletedbyinference = Boolean.parseBoolean(n.valueOf("./oaf:deletedbyinference"));
		final String inferenceprovenance = n.valueOf("./oaf:inferenceprovenance");
		final Boolean inferred = Boolean.parseBoolean(n.valueOf("./oaf:inferred"));
		final Float trust = Float.parseFloat(n.valueOf("./oaf:trust"));

		final Qualifier pAction = qualifier(paClassId, paClassName, paSchemeId);

		return dataInfo(invisible, deletedbyinference, trust, inferenceprovenance, inferred, pAction);
	}

	protected List<String> prepareListFields(
		final Node node,
		final String xpath) {
		return prepareListString(node, xpath);
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

	protected Set<String> validateUrl(Collection<String> url) {
		UrlValidator urlValidator = UrlValidator.getInstance();
		if (Objects.isNull(url)) {
			return new HashSet<>();
		}
		return url
			.stream()
			.filter(u -> urlValidator.isValid(u))
			.collect(Collectors.toCollection(HashSet::new));
	}

}
