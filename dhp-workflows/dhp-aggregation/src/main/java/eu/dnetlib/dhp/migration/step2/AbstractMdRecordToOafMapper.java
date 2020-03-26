package eu.dnetlib.dhp.migration.step2;

import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.createOpenaireId;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.dataInfo;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.field;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.journal;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.keyValue;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.listFields;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.oaiIProvenance;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.qualifier;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.structuredProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentFactory;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;

import eu.dnetlib.dhp.schema.oaf.Author;
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

	protected final Map<String, String> code2name;

	protected static final Qualifier MAIN_TITLE_QUALIFIER = qualifier("main title", "main title", "dnet:dataCite_title", "dnet:dataCite_title");

	protected static final Qualifier PUBLICATION_RESULTTYPE_QUALIFIER =
			qualifier("publication", "publication", "dnet:result_typologies", "dnet:result_typologies");
	protected static final Qualifier DATASET_RESULTTYPE_QUALIFIER = qualifier("dataset", "dataset", "dnet:result_typologies", "dnet:result_typologies");
	protected static final Qualifier SOFTWARE_RESULTTYPE_QUALIFIER = qualifier("software", "software", "dnet:result_typologies", "dnet:result_typologies");
	protected static final Qualifier OTHER_RESULTTYPE_QUALIFIER = qualifier("other", "other", "dnet:result_typologies", "dnet:result_typologies");

	protected AbstractMdRecordToOafMapper(final Map<String, String> code2name) {
		this.code2name = code2name;
	}

	public List<Oaf> processMdRecord(final String xml) {
		try {
			final Map<String, String> nsContext = new HashMap<>();
			nsContext.put("dr", "http://www.driver-repository.eu/namespace/dr");
			nsContext.put("dri", "http://www.driver-repository.eu/namespace/dri");
			nsContext.put("oaf", "http://namespace.openaire.eu/oaf");
			nsContext.put("oai", "http://www.openarchives.org/OAI/2.0/");
			nsContext.put("prov", "http://www.openarchives.org/OAI/2.0/provenance");
			nsContext.put("dc", "http://purl.org/dc/elements/1.1/");
			nsContext.put("datacite", "http://datacite.org/schema/kernel-3");
			DocumentFactory.getInstance().setXPathNamespaceURIs(nsContext);

			final Document doc = DocumentHelper.parseText(xml);

			final String type = doc.valueOf("//dr:CobjCategory/@type");
			final KeyValue collectedFrom = keyValue(doc.valueOf("//oaf:collectedFrom/@id"), doc.valueOf("//oaf:collectedFrom/@name"));
			final KeyValue hostedBy = StringUtils.isBlank(doc.valueOf("//oaf:hostedBy/@id")) ? collectedFrom
					: keyValue(doc.valueOf("//oaf:hostedBy/@id"), doc.valueOf("//oaf:hostedBy/@name"));

			final DataInfo info = prepareDataInfo(doc);
			final long lastUpdateTimestamp = new Date().getTime();

			return createOafs(doc, type, collectedFrom, hostedBy, info, lastUpdateTimestamp);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected List<Oaf> createOafs(final Document doc,
			final String type,
			final KeyValue collectedFrom,
			final KeyValue hostedBy,
			final DataInfo info,
			final long lastUpdateTimestamp) {

		final List<Oaf> oafs = new ArrayList<>();

		switch (type.toLowerCase()) {
		case "":
		case "publication":
			final Publication p = new Publication();
			populateResultFields(p, doc, collectedFrom, hostedBy, info, lastUpdateTimestamp);
			p.setResulttype(PUBLICATION_RESULTTYPE_QUALIFIER);
			p.setJournal(prepareJournal(doc, info));
			oafs.add(p);
			break;
		case "dataset":
			final Dataset d = new Dataset();
			populateResultFields(d, doc, collectedFrom, hostedBy, info, lastUpdateTimestamp);
			d.setResulttype(DATASET_RESULTTYPE_QUALIFIER);
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
			populateResultFields(s, doc, collectedFrom, hostedBy, info, lastUpdateTimestamp);
			s.setResulttype(SOFTWARE_RESULTTYPE_QUALIFIER);
			s.setDocumentationUrl(prepareSoftwareDocumentationUrls(doc, info));
			s.setLicense(prepareSoftwareLicenses(doc, info));
			s.setCodeRepositoryUrl(prepareSoftwareCodeRepositoryUrl(doc, info));
			s.setProgrammingLanguage(prepareSoftwareProgrammingLanguage(doc, info));
			oafs.add(s);
			break;
		case "otherresearchproducts":
		default:
			final OtherResearchProduct o = new OtherResearchProduct();
			populateResultFields(o, doc, collectedFrom, hostedBy, info, lastUpdateTimestamp);
			o.setResulttype(OTHER_RESULTTYPE_QUALIFIER);
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

	private List<Oaf> addProjectRels(final Document doc,
			final KeyValue collectedFrom,
			final DataInfo info,
			final long lastUpdateTimestamp) {

		final List<Oaf> res = new ArrayList<>();

		final String docId = createOpenaireId(50, doc.valueOf("//dri:objIdentifier"), false);

		for (final Object o : doc.selectNodes("//oaf:projectid")) {
			final String projectId = createOpenaireId(40, ((Node) o).getText(), true);

			final Relation r1 = new Relation();
			r1.setRelType("resultProject");
			r1.setSubRelType("outcome");
			r1.setRelClass("isProducedBy");
			r1.setSource(docId);
			r1.setTarget(projectId);
			r1.setCollectedFrom(Arrays.asList(collectedFrom));
			r1.setDataInfo(info);
			r1.setLastupdatetimestamp(lastUpdateTimestamp);
			res.add(r1);

			final Relation r2 = new Relation();
			r2.setRelType("resultProject");
			r2.setSubRelType("outcome");
			r2.setRelClass("produces");
			r2.setSource(projectId);
			r2.setTarget(docId);
			r2.setCollectedFrom(Arrays.asList(collectedFrom));
			r2.setDataInfo(info);
			r2.setLastupdatetimestamp(lastUpdateTimestamp);
			res.add(r2);
		}

		return res;
	}

	protected abstract List<Oaf> addOtherResultRels(final Document doc,
			final KeyValue collectedFrom,
			final DataInfo info,
			final long lastUpdateTimestamp);

	private void populateResultFields(final Result r,
			final Document doc,
			final KeyValue collectedFrom,
			final KeyValue hostedBy,
			final DataInfo info,
			final long lastUpdateTimestamp) {
		r.setDataInfo(info);
		r.setLastupdatetimestamp(lastUpdateTimestamp);
		r.setId(createOpenaireId(50, doc.valueOf("//dri:objIdentifier"), false));
		r.setOriginalId(Arrays.asList(doc.valueOf("//dri:objIdentifier")));
		r.setCollectedfrom(Arrays.asList(collectedFrom));
		r.setPid(prepareListStructProps(doc, "//oaf:identifier", "@identifierType", "dnet:pid_types", "dnet:pid_types", info));
		r.setDateofcollection(doc.valueOf("//dr:dateOfCollection"));
		r.setDateoftransformation(doc.valueOf("//dr:dateOfTransformation"));
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
		r.setContext(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r.setExternalReference(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r.setInstance(prepareInstances(doc, info, collectedFrom, hostedBy));
	}

	protected abstract Qualifier prepareResourceType(Document doc, DataInfo info);

	protected abstract List<Instance> prepareInstances(Document doc, DataInfo info, KeyValue collectedfrom, KeyValue hostedby);

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

	protected abstract List<Field<String>> prepareOtherResearchProductTools(Document doc, DataInfo info);

	protected abstract List<Field<String>> prepareOtherResearchProductContactGroups(Document doc, DataInfo info);

	protected abstract List<Field<String>> prepareOtherResearchProductContactPersons(Document doc, DataInfo info);

	protected abstract Qualifier prepareSoftwareProgrammingLanguage(Document doc, DataInfo info);

	protected abstract Field<String> prepareSoftwareCodeRepositoryUrl(Document doc, DataInfo info);

	protected abstract List<StructuredProperty> prepareSoftwareLicenses(Document doc, DataInfo info);

	protected abstract List<Field<String>> prepareSoftwareDocumentationUrls(Document doc, DataInfo info);

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
			if (StringUtils.isNotBlank(name)) { return journal(name, issnPrinted, issnOnline, issnLinking, ep, iss, sp, vol, edition, null, null, info); }
		}
		return null;
	}

	protected Qualifier prepareQualifier(final Node node, final String xpath, final String schemeId, final String schemeName) {
		final String classId = node.valueOf(xpath);
		final String className = code2name.get(classId);
		return qualifier(classId, className, schemeId, schemeName);
	}

	protected List<StructuredProperty> prepareListStructProps(final Node node,
			final String xpath,
			final String xpathClassId,
			final String schemeId,
			final String schemeName,
			final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : node.selectNodes(xpath)) {
			final Node n = (Node) o;
			final String classId = n.valueOf(xpathClassId);
			final String className = code2name.get(classId);
			res.add(structuredProperty(n.getText(), classId, className, schemeId, schemeName, info));
		}
		return res;
	}

	protected List<StructuredProperty> prepareListStructProps(final Node node, final String xpath, final Qualifier qualifier, final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : node.selectNodes(xpath)) {
			final Node n = (Node) o;
			res.add(structuredProperty(n.getText(), qualifier, info));
		}
		return res;
	}

	protected List<StructuredProperty> prepareListStructProps(final Node node, final String xpath, final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : node.selectNodes(xpath)) {
			final Node n = (Node) o;
			res.add(structuredProperty(n.getText(), n.valueOf("@classid"), n.valueOf("@classname"), n.valueOf("@schemeid"), n
					.valueOf("@schemename"), info));
		}
		return res;
	}

	protected OAIProvenance prepareOAIprovenance(final Document doc) {
		final Node n = doc.selectSingleNode("//*[local-name()='provenance']/*[local-name()='originDescription']");

		if (n == null) { return null; }

		final String identifier = n.valueOf("./*[local-name()='identifier']");
		final String baseURL = n.valueOf("./*[local-name()='baseURL']");;
		final String metadataNamespace = n.valueOf("./*[local-name()='metadataNamespace']");;
		final boolean altered = n.valueOf("@altered").equalsIgnoreCase("true");
		final String datestamp = n.valueOf("./*[local-name()='datestamp']");;
		final String harvestDate = n.valueOf("@harvestDate");;

		return oaiIProvenance(identifier, baseURL, metadataNamespace, altered, datestamp, harvestDate);

	}

	protected DataInfo prepareDataInfo(final Document doc) {
		final Node n = doc.selectSingleNode("//oaf:datainfo");

		if (n == null) { return null; }

		final String paClassId = n.valueOf("./oaf:provenanceaction/@classid");
		final String paClassName = n.valueOf("./oaf:provenanceaction/@classname");
		final String paSchemeId = n.valueOf("./oaf:provenanceaction/@schemeid");
		final String paSchemeName = n.valueOf("./oaf:provenanceaction/@schemename");

		final boolean deletedbyinference = Boolean.parseBoolean(n.valueOf("./oaf:deletedbyinference"));
		final String inferenceprovenance = n.valueOf("./oaf:inferenceprovenance");
		final Boolean inferred = Boolean.parseBoolean(n.valueOf("./oaf:inferred"));
		final String trust = n.valueOf("./oaf:trust");

		return dataInfo(deletedbyinference, inferenceprovenance, inferred, false, qualifier(paClassId, paClassName, paSchemeId, paSchemeName), trust);
	}

	protected Field<String> prepareField(final Node node, final String xpath, final DataInfo info) {
		return field(node.valueOf(xpath), info);
	}

	protected List<Field<String>> prepareListFields(final Node node, final String xpath, final DataInfo info) {
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
