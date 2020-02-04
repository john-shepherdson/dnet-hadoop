package eu.dnetlib.dhp.migration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.Node;

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.GeoLocation;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class OafMigrationExecutor extends AbstractMongoExecutor {

	public OafMigrationExecutor(final String hdfsPath, final String hdfsNameNode, final String hdfsUser, final String mongoBaseUrl, final String mongoDb,
			final String dbUrl, final String dbUser,
			final String dbPassword) throws Exception {
		super(hdfsPath, hdfsNameNode, hdfsUser, mongoBaseUrl, mongoDb, dbUrl, dbUser, dbPassword);
	}

	private static final Log log = LogFactory.getLog(MigrateMongoMdstoresApplication.class);

	@Override
	protected void registerNamespaces(final Map<String, String> nsContext) {
		nsContext.put("dc", "http://purl.org/dc/elements/1.1/");
		nsContext.put("dr", "http://www.driver-repository.eu/namespace/dr");
		nsContext.put("dri", "http://www.driver-repository.eu/namespace/dri");
		nsContext.put("oaf", "http://namespace.openaire.eu/oaf");
		nsContext.put("oai", "http://www.openarchives.org/OAI/2.0/");
		nsContext.put("prov", "http://www.openarchives.org/OAI/2.0/provenance");
	}

	@Override
	protected void addRelations(final List<Oaf> oafs,
			final Document doc,
			final String type,
			final KeyValue collectedFrom,
			final DataInfo info,
			final long lastUpdateTimestamp) {
		for (final Object o : doc.selectNodes("//")) { // TODO
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

	@Override
	protected List<Author> prepareAuthors(final Document doc, final DataInfo info) {
		final List<Author> res = new ArrayList<>();
		int pos = 1;
		for (final Object o : doc.selectNodes("//dc:creator")) {
			final Node n = (Node) o;
			final Author author = new Author();
			author.setFullname(n.getText());
			author.setRank(pos++);
		}
		return res;
	}

	@Override
	protected Qualifier prepareLanguages(final Document doc) {
		return prepareQualifier(doc, "//dc:language", "dnet:languages", "dnet:languages");
	}

	@Override
	protected List<StructuredProperty> prepareSubjects(final Document doc, final DataInfo info) {
		return prepareListStructProps(doc, "//dc:subject", info);
	}

	@Override
	protected List<StructuredProperty> prepareTitles(final Document doc, final DataInfo info) {
		return prepareListStructProps(doc, "//dc:title", MAIN_TITLE_QUALIFIER, info);
	}

	@Override
	protected List<Field<String>> prepareDescriptions(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:description", info);
	}

	@Override
	protected Field<String> preparePublisher(final Document doc, final DataInfo info) {
		return prepareField(doc, "//dc:publisher", info);
	}

	@Override
	protected List<Field<String>> prepareFormats(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:format", info);
	}

	@Override
	protected List<Field<String>> prepareContributors(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:contributor", info);
	}

	@Override
	protected List<Field<String>> prepareCoverages(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:coverage", info);
	}

	@Override
	protected List<Instance> prepareInstances(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<Field<String>> prepareSources(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:source", info);
	}

	@Override
	protected Field<String> prepareEmbargoEndDate(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<StructuredProperty> prepareRelevantDates(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductTools(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactGroups(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactPersons(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Qualifier prepareSoftwareProgrammingLanguage(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Field<String> prepareSoftwareCodeRepositoryUrl(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<StructuredProperty> prepareSoftwareLicenses(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<Field<String>> prepareSoftwareDocumentationUrls(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<GeoLocation> prepareDatasetGeoLocations(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Field<String> prepareDatasetMetadataVersionNumber(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Field<String> prepareDatasetLastMetadataUpdate(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Field<String> prepareDatasetVersion(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Field<String> prepareDatasetSize(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Field<String> prepareDatasetDevice(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Field<String> prepareDatasetStorageDate(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
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

}
