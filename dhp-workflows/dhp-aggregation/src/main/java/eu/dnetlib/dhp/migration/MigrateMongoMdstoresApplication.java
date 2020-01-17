package eu.dnetlib.dhp.migration;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;

public class MigrateMongoMdstoresApplication extends AbstractMigrateApplication implements Closeable {

	private static final Log log = LogFactory.getLog(MigrateMongoMdstoresApplication.class);

	private final MdstoreClient mdstoreClient;

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

		try (final MigrateMongoMdstoresApplication mig = new MigrateMongoMdstoresApplication(hdfsPath, hdfsNameNode, hdfsUser, mongoBaseUrl, mongoDb)) {
			mig.processMdRecords(mdFormat, mdLayout, mdInterpretation);
		}

	}

	public MigrateMongoMdstoresApplication(final String hdfsPath, final String hdfsNameNode, final String hdfsUser, final String mongoBaseUrl,
			final String mongoDb) throws Exception {
		super(hdfsPath, hdfsNameNode, hdfsUser);
		this.mdstoreClient = new MdstoreClient(mongoBaseUrl, mongoDb);

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
		final SAXReader reader = new SAXReader();
		final Document doc = reader.read(new StringReader(xml));

		final String type = doc.valueOf(""); // TODO

		final List<Oaf> oafs = new ArrayList<>();

		switch (type.toLowerCase()) {
		case "publication":
			final Publication p = new Publication();
			populateResultFields(p, doc);
			p.setJournal(null); // TODO
			oafs.add(p);
			break;
		case "dataset":
			final Dataset d = new Dataset();
			populateResultFields(d, doc);
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
			final OtherResearchProduct o = new OtherResearchProduct();
			populateResultFields(o, doc);
			o.setContactperson(null); // TODO
			o.setContactgroup(null); // TODO
			o.setTool(null); // TODO
			oafs.add(o);
			break;
		case "software":
			final Software s = new Software();
			populateResultFields(s, doc);
			s.setDocumentationUrl(null); // TODO
			s.setLicense(null); // TODO
			s.setCodeRepositoryUrl(null); // TODO
			s.setProgrammingLanguage(null); // TODO
			oafs.add(s);
			break;
		default:
			log.error("Inavlid type: " + type);
			break;
		}

		if (!oafs.isEmpty()) {
			addRelations(oafs, doc, "//*", "TYPE");
			addRelations(oafs, doc, "//*", "TYPE");
			addRelations(oafs, doc, "//*", "TYPE");
		}

		return oafs;
	}

	private void addRelations(final List<Oaf> oafs, final Document doc, final String xpath, final String type) {
		for (final Object o : doc.selectNodes(xpath)) {
			final Node n = (Node) o;
			final Relation r = new Relation();
			r.setRelType(null); // TODO
			r.setSubRelType(null); // TODO
			r.setRelClass(null); // TODO
			r.setSource(null); // TODO
			r.setTarget(null); // TODO
			r.setCollectedFrom(null); // TODO
			oafs.add(r);
		}

	}

	private void populateResultFields(final Result r, final Document doc) {
		r.setDataInfo(null); // TODO
		r.setLastupdatetimestamp(null); // TODO
		r.setId(null); // TODO
		r.setOriginalId(null); // TODO
		r.setCollectedfrom(null); // TODO
		r.setPid(null); // TODO
		r.setDateofcollection(null); // TODO
		r.setDateoftransformation(null); // TODO
		r.setExtraInfo(null); // TODO
		r.setOaiprovenance(null); // TODO
		r.setAuthor(null); // TODO
		r.setResulttype(null); // TODO
		r.setLanguage(null); // TODO
		r.setCountry(null); // TODO
		r.setSubject(null); // TODO
		r.setTitle(null); // TODO
		r.setRelevantdate(null); // TODO
		r.setDescription(null); // TODO
		r.setDateofacceptance(null); // TODO
		r.setPublisher(null); // TODO
		r.setEmbargoenddate(null); // TODO
		r.setSource(null); // TODO
		r.setFulltext(null); // TODO
		r.setFormat(null); // TODO
		r.setContributor(null); // TODO
		r.setResourcetype(null); // TODO
		r.setCoverage(null); // TODO
		r.setRefereed(null); // TODO
		r.setContext(null); // TODO
		r.setExternalReference(null); // TODO
		r.setInstance(null); // TODO
		r.setProcessingchargeamount(null); // TODO
		r.setProcessingchargecurrency(null); // TODO
	}

	@Override
	public void close() throws IOException {
		super.close();
		mdstoreClient.close();
	}
}
