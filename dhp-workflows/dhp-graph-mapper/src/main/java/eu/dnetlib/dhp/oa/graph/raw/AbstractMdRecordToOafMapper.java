
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.schema.common.ModelConstants.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.createOpenaireId;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.dom4j.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.common.Constants;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

public abstract class AbstractMdRecordToOafMapper {

	protected final VocabularyGroup vocs;

	protected static final UrlValidator URL_VALIDATOR = new UrlValidator(UrlValidator.ALLOW_2_SLASHES);

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
		DNET_PID_TYPES, DNET_PID_TYPES);
	protected static final Qualifier MAG_PID_TYPE = qualifier(
		"MAGIdentifier", "Microsoft Academic Graph Identifier", DNET_PID_TYPES, DNET_PID_TYPES);

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

			final DataInfo entityInfo = prepareDataInfo(doc, invisible);
			final long lastUpdateTimestamp = new Date().getTime();

			final List<Instance> instances = prepareInstances(doc, entityInfo, collectedFrom, hostedBy);

			final String type = getResultType(doc, instances);

			return createOafs(doc, type, instances, collectedFrom, entityInfo, lastUpdateTimestamp);
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
		final DataInfo info,
		final long lastUpdateTimestamp) {

		final OafEntity entity = createEntity(doc, type, instances, collectedFrom, info, lastUpdateTimestamp);

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

		final DataInfo relationInfo = prepareDataInfo(doc, false);

		if (!oafs.isEmpty()) {
			Set<Oaf> rels = Sets.newHashSet();

			rels.addAll(addProjectRels(doc, entity, relationInfo));
			rels.addAll(addOtherResultRels(doc, entity, relationInfo));
			rels.addAll(addRelations(doc, entity, relationInfo));
			rels.addAll(addAffiliations(doc, entity, relationInfo));

			oafs.addAll(rels);
		}

		return oafs;
	}

	private OafEntity createEntity(final Document doc,
		final String type,
		final List<Instance> instances,
		final KeyValue collectedFrom,
		final DataInfo info,
		final long lastUpdateTimestamp) {
		switch (type.toLowerCase()) {
			case "publication":
				final Publication p = new Publication();
				populateResultFields(p, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				p.setJournal(prepareJournal(doc, info));
				return p;
			case "dataset":
				final Dataset d = new Dataset();
				populateResultFields(d, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				d.setStoragedate(prepareDatasetStorageDate(doc, info));
				d.setDevice(prepareDatasetDevice(doc, info));
				d.setSize(prepareDatasetSize(doc, info));
				d.setVersion(prepareDatasetVersion(doc, info));
				d.setLastmetadataupdate(prepareDatasetLastMetadataUpdate(doc, info));
				d.setMetadataversionnumber(prepareDatasetMetadataVersionNumber(doc, info));
				d.setGeolocation(prepareDatasetGeoLocations(doc, info));
				return d;
			case "software":
				final Software s = new Software();
				populateResultFields(s, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				s.setDocumentationUrl(prepareSoftwareDocumentationUrls(doc, info));
				s.setLicense(prepareSoftwareLicenses(doc, info));
				s.setCodeRepositoryUrl(prepareSoftwareCodeRepositoryUrl(doc, info));
				s.setProgrammingLanguage(prepareSoftwareProgrammingLanguage(doc, info));
				return s;
			case "":
			case "otherresearchproducts":
			default:
				final OtherResearchProduct o = new OtherResearchProduct();
				populateResultFields(o, doc, instances, collectedFrom, info, lastUpdateTimestamp);
				o.setContactperson(prepareOtherResearchProductContactPersons(doc, info));
				o.setContactgroup(prepareOtherResearchProductContactGroups(doc, info));
				o.setTool(prepareOtherResearchProductTools(doc, info));
				return o;
		}
	}

	private List<Oaf> addProjectRels(
		final Document doc,
		final OafEntity entity, DataInfo info) {

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
							.getRelation(
								docId, projectId, RESULT_PROJECT, OUTCOME, IS_PRODUCED_BY, entity.getCollectedfrom(),
								info, entity.getLastupdatetimestamp(), validationdDate, null));
				res
					.add(
						OafMapperUtils
							.getRelation(
								projectId, docId, RESULT_PROJECT, OUTCOME, PRODUCES, entity.getCollectedfrom(), info,
								entity.getLastupdatetimestamp(), validationdDate, null));
			}
		}

		return res;
	}

	private List<Oaf> addRelations(Document doc, OafEntity entity, DataInfo info) {

		final List<Oaf> rels = Lists.newArrayList();

		for (Object o : doc.selectNodes("//oaf:relation")) {
			Element element = (Element) o;

			final String target = StringUtils.trim(element.getText());
			final String relType = element.attributeValue("relType");
			final String subRelType = element.attributeValue("subRelType");
			final String relClass = element.attributeValue("relClass");

			if (StringUtils.isNotBlank(target) && StringUtils.isNotBlank(relType) && StringUtils.isNotBlank(subRelType)
				&& StringUtils.isNotBlank(relClass)) {

				final String relClassInverse = ModelSupport
					.findInverse(ModelSupport.rel(relType, subRelType, relClass))
					.getInverseRelClass();
				final String validationDate = ((Node) o).valueOf("@validationDate");

				if (StringUtils.isNotBlank(target)) {
					final String targetType = element.attributeValue("targetType");
					if (StringUtils.isNotBlank(targetType)) {
						final String targetId = createOpenaireId(targetType, target, true);
						rels
							.add(
								OafMapperUtils
									.getRelation(
										entity.getId(), targetId, relType, subRelType, relClass,
										entity.getCollectedfrom(), info,
										entity.getLastupdatetimestamp(), validationDate, null));
						rels
							.add(
								OafMapperUtils
									.getRelation(
										targetId, entity.getId(), relType, subRelType, relClassInverse,
										entity.getCollectedfrom(), info,
										entity.getLastupdatetimestamp(), validationDate, null));
					}
				}
			}
		}
		return rels;
	}

	private List<Oaf> addAffiliations(Document doc, OafEntity entity, DataInfo info) {
		final List<Oaf> rels = Lists.newArrayList();

		for (Object o : doc.selectNodes("//datacite:affiliation[@affiliationIdentifierScheme='ROR']")) {
			Element element = (Element) o;

			String rorId = element.attributeValue("affiliationIdentifier");
			if (StringUtils.isNotBlank(rorId)) {

				String fullRorId = Constants.ROR_NS_PREFIX + "::" + rorId;

				String resultId = entity.getId();
				String orgId = createOpenaireId("organization", fullRorId, true);

				List<KeyValue> properties = Lists.newArrayList();

				String apcAmount = doc.valueOf("//oaf:processingchargeamount");
				String apcCurrency = doc.valueOf("//oaf:processingchargeamount/@currency");

				if (StringUtils.isNotBlank(apcAmount) && StringUtils.isNotBlank(apcCurrency)) {
					properties.add(OafMapperUtils.keyValue("apc_amount", apcAmount));
					properties.add(OafMapperUtils.keyValue("apc_currency", apcCurrency));
				}

				rels
					.add(
						OafMapperUtils
							.getRelation(
								resultId, orgId, RESULT_ORGANIZATION, AFFILIATION, HAS_AUTHOR_INSTITUTION,
								entity.getCollectedfrom(), info, entity.getLastupdatetimestamp(), null,
								properties));
				rels
					.add(
						OafMapperUtils
							.getRelation(
								orgId, resultId, RESULT_ORGANIZATION, AFFILIATION, IS_AUTHOR_INSTITUTION_OF,
								entity.getCollectedfrom(), info, entity.getLastupdatetimestamp(), null,
								properties));
			}
		}
		return rels;
	}

	protected abstract List<Oaf> addOtherResultRels(
		final Document doc,
		final OafEntity entity, DataInfo info);

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
		r.setPid(IdentifierFactory.getPids(prepareResultPids(doc, info), collectedFrom));
		r.setDateofcollection(doc.valueOf("//dr:dateOfCollection/text()|//dri:dateOfCollection/text()"));
		r.setDateoftransformation(doc.valueOf("//dr:dateOfTransformation/text()|//dri:dateOfTransformation/text()"));
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
		r.setFulltext(prepareListURL(doc, "//oaf:fulltext", info));
		r.setFormat(prepareFormats(doc, info));
		r.setContributor(prepareContributors(doc, info));
		r.setResourcetype(prepareResourceType(doc, info));
		r.setCoverage(prepareCoverages(doc, info));
		r.setContext(prepareContexts(doc, info));
		r.setExternalReference(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r
			.setProcessingchargeamount(field(doc.valueOf("//oaf:processingchargeamount"), info));
		r
			.setProcessingchargecurrency(field(doc.valueOf("//oaf:processingchargeamount/@currency"), info));

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

	protected abstract List<Subject> prepareSubjects(Document doc, DataInfo info);

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
		accessRight.setSchemename(qualifier.getSchemename());

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

	protected List<Subject> prepareSubjectList(
		final Node node,
		final String xpath,
		final DataInfo info) {
		final List<Subject> res = new ArrayList<>();
		for (final Object o : node.selectNodes(xpath)) {
			final Node n = (Node) o;
			res
				.add(
					subject(
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

	protected List<Field<String>> prepareListURL(final Node node, final String xpath, final DataInfo info) {
		return listFields(
			info, prepareListString(node, xpath)
				.stream()
				.filter(URL_VALIDATOR::isValid)
				.collect(Collectors.toList()));
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

	protected Set<String> validateUrl(Collection<String> url) {

		if (Objects.isNull(url)) {
			return new HashSet<>();
		}
		return url
			.stream()
			.filter(URL_VALIDATOR::isValid)
			.collect(Collectors.toCollection(HashSet::new));
	}

}
