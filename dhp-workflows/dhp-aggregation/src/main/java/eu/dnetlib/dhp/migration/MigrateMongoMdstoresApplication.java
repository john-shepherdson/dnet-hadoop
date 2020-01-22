package eu.dnetlib.dhp.migration;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentFactory;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Field;
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

public class MigrateMongoMdstoresApplication extends AbstractMigrateApplication implements Closeable {

	private static final Log log = LogFactory.getLog(MigrateMongoMdstoresApplication.class);

	private final Map<String, String> code2name = new HashMap<>();

	private final MdstoreClient mdstoreClient;

	private static final Qualifier MAIN_TITLE_QUALIFIER = qualifier("main title", "main title", "dnet:dataCite_title", "dnet:dataCite_title");

	private static final Qualifier PUBLICATION_RESULTTYPE_QUALIFIER =
			qualifier("publication", "publication", "dnet:result_typologies", "dnet:result_typologies");
	private static final Qualifier DATASET_RESULTTYPE_QUALIFIER = qualifier("dataset", "dataset", "dnet:result_typologies", "dnet:result_typologies");
	private static final Qualifier SOFTWARE_RESULTTYPE_QUALIFIER = qualifier("software", "software", "dnet:result_typologies", "dnet:result_typologies");
	private static final Qualifier OTHER_RESULTTYPE_QUALIFIER = qualifier("other", "other", "dnet:result_typologies", "dnet:result_typologies");

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils.toString(MigrateMongoMdstoresApplication.class.getResourceAsStream("/eu/dnetlib/dhp/migration/migrate_mongo_mstores_parameters.json")));
		parser.parseArgument(args);

		final String mongoBaseUrl = parser.get("mongoBaseUrl");
		final String mongoDb = parser.get("mongoDb");

		final String mdFormat = parser.get("mdFormat");
		final String mdLayout = parser.get("mdLayout");
		final String mdInterpretation = parser.get("mdInterpretation");

		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("namenode");
		final String hdfsUser = parser.get("hdfsUser");

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");

		try (final MigrateMongoMdstoresApplication mig =
				new MigrateMongoMdstoresApplication(hdfsPath, hdfsNameNode, hdfsUser, mongoBaseUrl, mongoDb, dbUrl, dbUser, dbPassword)) {
			mig.processMdRecords(mdFormat, mdLayout, mdInterpretation);
		}

	}

	public MigrateMongoMdstoresApplication(final String hdfsPath, final String hdfsNameNode, final String hdfsUser, final String mongoBaseUrl,
			final String mongoDb, final String dbUrl, final String dbUser,
			final String dbPassword) throws Exception {
		super(hdfsPath, hdfsNameNode, hdfsUser);

		this.mdstoreClient = new MdstoreClient(mongoBaseUrl, mongoDb);
		loadClassNames(dbUrl, dbUser, dbPassword);

		final Map<String, String> nsContext = new HashMap<>();
		nsContext.put("dc", "http://purl.org/dc/elements/1.1/");
		nsContext.put("dr", "http://www.driver-repository.eu/namespace/dr");
		nsContext.put("dri", "http://www.driver-repository.eu/namespace/dri");
		nsContext.put("oaf", "http://namespace.openaire.eu/oaf");
		nsContext.put("oai", "http://www.openarchives.org/OAI/2.0/");
		nsContext.put("prov", "http://www.openarchives.org/OAI/2.0/provenance");
		DocumentFactory.getInstance().setXPathNamespaceURIs(nsContext);
	}

	private void loadClassNames(final String dbUrl, final String dbUser, final String dbPassword) throws IOException {
		try (DbClient dbClient = new DbClient(dbUrl, dbUser, dbPassword)) {
			code2name.clear();
			dbClient.processResults("select code, name from class", rs -> {
				try {
					code2name.put(rs.getString("code"), rs.getString("name"));
				} catch (final SQLException e) {
					e.printStackTrace();
				}
			});
		}

	}

	public void processMdRecords(final String mdFormat, final String mdLayout, final String mdInterpretation) throws DocumentException {

		for (final Entry<String, String> entry : mdstoreClient.validCollections(mdFormat, mdLayout, mdInterpretation).entrySet()) {
			// final String mdId = entry.getKey();
			final String currentColl = entry.getValue();

			for (final String xml : mdstoreClient.listRecords(currentColl)) {
				for (final Oaf oaf : createOafs(xml)) {
					emitOaf(oaf);
				}
			}
		}
	}

	private List<Oaf> createOafs(final String xml) throws DocumentException {

		final Document doc = DocumentHelper.parseText(xml);

		final String type = doc.valueOf("//dr:CobjCategory/@type");
		final KeyValue collectedFrom = keyValue(doc.valueOf("//oaf:collectedFrom/@id"), doc.valueOf("//oaf:collectedFrom/@name"));
		final DataInfo info = prepareDataInfo(doc);
		final long lastUpdateTimestamp = new Date().getTime();

		final List<Oaf> oafs = new ArrayList<>();

		switch (type.toLowerCase()) {
		case "":
		case "publication":
			final Publication p = new Publication();
			populateResultFields(p, doc, collectedFrom, info, lastUpdateTimestamp);
			p.setResulttype(PUBLICATION_RESULTTYPE_QUALIFIER);
			p.setJournal(null); // TODO
			oafs.add(p);
			break;
		case "dataset":
			final Dataset d = new Dataset();
			populateResultFields(d, doc, collectedFrom, info, lastUpdateTimestamp);
			d.setResulttype(DATASET_RESULTTYPE_QUALIFIER);
			d.setStoragedate(null); // TODO
			d.setDevice(null); // TODO
			d.setSize(null); // TODO
			d.setVersion(null); // TODO
			d.setLastmetadataupdate(null); // TODO
			d.setMetadataversionnumber(null); // TODO
			d.setGeolocation(null); // TODO
			oafs.add(d);
			break;
		case "otherresearchproducts":

		case "software":
			final Software s = new Software();
			populateResultFields(s, doc, collectedFrom, info, lastUpdateTimestamp);
			s.setResulttype(SOFTWARE_RESULTTYPE_QUALIFIER);
			s.setDocumentationUrl(null); // TODO
			s.setLicense(null); // TODO
			s.setCodeRepositoryUrl(null); // TODO
			s.setProgrammingLanguage(null); // TODO
			oafs.add(s);
			break;
		default:
			final OtherResearchProduct o = new OtherResearchProduct();
			populateResultFields(o, doc, collectedFrom, info, lastUpdateTimestamp);
			o.setResulttype(OTHER_RESULTTYPE_QUALIFIER);
			o.setContactperson(null); // TODO
			o.setContactgroup(null); // TODO
			o.setTool(null); // TODO
			oafs.add(o);
			break;
		}

		if (!oafs.isEmpty()) {
			addRelations(oafs, doc, "//*", "TYPE", collectedFrom, info, lastUpdateTimestamp); // TODO
			addRelations(oafs, doc, "//*", "TYPE", collectedFrom, info, lastUpdateTimestamp); // TODO
			addRelations(oafs, doc, "//*", "TYPE", collectedFrom, info, lastUpdateTimestamp); // TODO
		}

		return oafs;
	}

	private void addRelations(final List<Oaf> oafs,
			final Document doc,
			final String xpath,
			final String type,
			final KeyValue collectedFrom,
			final DataInfo info,
			final long lastUpdateTimestamp) {
		for (final Object o : doc.selectNodes(xpath)) {
			final Node n = (Node) o;
			final Relation r = new Relation();
			r.setRelType(null); // TODO
			r.setSubRelType(null); // TODO
			r.setRelClass(null); // TODO
			r.setSource(null); // TODO
			r.setTarget(null); // TODO
			r.setCollectedFrom(Arrays.asList(collectedFrom));
			r.setDataInfo(info);
			r.setLastupdatetimestamp(lastUpdateTimestamp);
			oafs.add(r);
		}

	}

	private void populateResultFields(final Result r, final Document doc, final KeyValue collectedFrom, final DataInfo info, final long lastUpdateTimestamp) {

		r.setDataInfo(info);
		r.setLastupdatetimestamp(lastUpdateTimestamp);
		r.setId(createOpenaireId(50, doc.valueOf("//dri:objIdentifier")));
		r.setOriginalId(Arrays.asList(doc.valueOf("//dri:objIdentifier")));
		r.setCollectedfrom(Arrays.asList(collectedFrom));
		r.setPid(prepareListStructProps(doc, "//oaf:identifier", "@identifierType", "dnet:pid_types", "dnet:pid_types", info));
		r.setDateofcollection(doc.valueOf("//dr:dateOfCollection"));
		r.setDateoftransformation(doc.valueOf("//dr:dateOfTransformation"));
		r.setExtraInfo(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r.setOaiprovenance(prepareOAIprovenance(doc));
		r.setAuthor(null); // TODO
		r.setLanguage(prepareQualifier(doc, "//dc:language", "dnet:languages", "dnet:languages"));
		r.setCountry(new ArrayList<>()); // NOT PRESENT IN MDSTORES
		r.setSubject(prepareListStructProps(doc, "//dc:subject", info));
		r.setTitle(prepareListStructProps(doc, "//dc:title", MAIN_TITLE_QUALIFIER, info));
		r.setRelevantdate(null); // TODO
		r.setDescription(prepareListFields(doc, "//dc:description", info));
		r.setDateofacceptance(prepareField(doc, "//oaf:dateAccepted", info));
		r.setPublisher(prepareField(doc, "//dc:publisher", info));
		r.setEmbargoenddate(null); // TODO
		r.setSource(null); // TODO
		r.setFulltext(null); // TODO
		r.setFormat(prepareListFields(doc, "//dc:format", info));
		r.setContributor(prepareListFields(doc, "//dc:contributor", info));
		r.setResourcetype(null); // TODO
		r.setCoverage(prepareListFields(doc, "//dc:coverage", info));
		r.setRefereed(null); // TODO
		r.setContext(null); // TODO
		r.setExternalReference(null); // TODO
		r.setInstance(null); // TODO
		r.setProcessingchargeamount(null); // TODO
		r.setProcessingchargecurrency(null); // TODO
	}

	private Qualifier prepareQualifier(final Document doc, final String xpath, final String schemeId, final String schemeName) {
		final String classId = doc.valueOf(xpath);
		final String className = code2name.get(classId);
		return qualifier(classId, className, schemeId, schemeName);
	}

	private List<StructuredProperty> prepareListStructProps(final Document doc,
			final String xpath,
			final String xpathClassId,
			final String schemeId,
			final String schemeName,
			final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : doc.selectNodes(xpath)) {
			final Node n = (Node) o;
			final String classId = n.valueOf(xpathClassId);
			final String className = code2name.get(classId);
			res.add(structuredProperty(n.getText(), classId, className, schemeId, schemeName, info));
		}
		return res;
	}

	private List<StructuredProperty> prepareListStructProps(final Document doc, final String xpath, final Qualifier qualifier, final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : doc.selectNodes(xpath)) {
			final Node n = (Node) o;
			res.add(structuredProperty(n.getText(), qualifier, info));
		}
		return res;
	}

	private List<StructuredProperty> prepareListStructProps(final Document doc, final String xpath, final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : doc.selectNodes(xpath)) {
			final Node n = (Node) o;
			res.add(structuredProperty(n.getText(), n.valueOf("@classid"), n.valueOf("@classname"), n.valueOf("@schemeid"), n
					.valueOf("@schemename"), info));
		}
		return res;
	}

	private OAIProvenance prepareOAIprovenance(final Document doc) {
		final Node n = doc.selectSingleNode("//*[local-name()='provenance']/*[local-name()='originDescription']");

		final String identifier = n.valueOf("./*[local-name()='identifier']");
		final String baseURL = n.valueOf("./*[local-name()='baseURL']");;
		final String metadataNamespace = n.valueOf("./*[local-name()='metadataNamespace']");;
		final boolean altered = n.valueOf("@altered").equalsIgnoreCase("true");
		final String datestamp = n.valueOf("./*[local-name()='datestamp']");;
		final String harvestDate = n.valueOf("@harvestDate");;

		return oaiIProvenance(identifier, baseURL, metadataNamespace, altered, datestamp, harvestDate);
	}

	private DataInfo prepareDataInfo(final Document doc) {
		final Node n = doc.selectSingleNode("//oaf:datainfo");

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

	private Field<String> prepareField(final Document doc, final String xpath, final DataInfo info) {
		return field(doc.valueOf(xpath), info);
	}

	private List<Field<String>> prepareListFields(final Document doc, final String xpath, final DataInfo info) {
		return listFields(info, (String[]) prepareListString(doc, xpath).toArray());
	}

	private List<String> prepareListString(final Document doc, final String xpath) {
		final List<String> res = new ArrayList<>();
		for (final Object o : doc.selectNodes(xpath)) {
			final String s = ((Node) o).getText().trim();
			if (StringUtils.isNotBlank(s)) {
				res.add(s);
			}
		}
		return res;
	}
	/*
	 * private StructuredProperty prepareStructProp(final Document doc, final String xpath, final DataInfo dataInfo) { if
	 * (StringUtils.isBlank(s)) { return null; } final String[] parts = s.split("###"); if (parts.length == 2) { final String value =
	 * parts[0]; final String[] arr = parts[1].split("@@@"); if (arr.length == 4) { return structuredProperty(value, arr[0], arr[1], arr[2],
	 * arr[3], dataInfo); } } return null; }
	 *
	 * private List<StructuredProperty> prepareListOfStructProps(final Document doc, final String xpath, final DataInfo dataInfo) { final
	 * List<StructuredProperty> res = new ArrayList<>(); if (array != null) { for (final String s : (String[]) array.getArray()) { final
	 * StructuredProperty sp = prepareStructProp(s, dataInfo); if (sp != null) { res.add(sp); } } }
	 *
	 * return res; }
	 *
	 * private Journal prepareJournal(final Document doc, final String xpath, final DataInfo info) { if (StringUtils.isNotBlank(sj)) { final
	 * String[] arr = sj.split("@@@"); if (arr.length == 3) { final String issn = StringUtils.isNotBlank(arr[0]) ? arr[0] : null; final
	 * String eissn = StringUtils.isNotBlank(arr[1]) ? arr[1] : null;; final String lissn = StringUtils.isNotBlank(arr[2]) ? arr[2] : null;;
	 * if (issn != null || eissn != null || lissn != null) { return journal(name, issn, eissn, eissn, null, null, null, null, null, null,
	 * null, info); } } } return null; }
	 */

	@Override
	public void close() throws IOException {
		super.close();
		mdstoreClient.close();
	}

}
