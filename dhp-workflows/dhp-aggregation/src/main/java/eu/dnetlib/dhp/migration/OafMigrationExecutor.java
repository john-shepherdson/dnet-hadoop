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

	private static final Log log = LogFactory.getLog(OafMigrationExecutor.class);

	public OafMigrationExecutor(final String hdfsPath, final String hdfsNameNode, final String hdfsUser, final String mongoBaseUrl, final String mongoDb,
			final String dbUrl, final String dbUser,
			final String dbPassword) throws Exception {
		super(hdfsPath, hdfsNameNode, hdfsUser, mongoBaseUrl, mongoDb, dbUrl, dbUser, dbPassword);
	}

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
			res.add(author);
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
	protected List<Instance> prepareInstances(final Document doc, final DataInfo info, final KeyValue collectedfrom, final KeyValue hostedby) {
		final List<Instance> res = new ArrayList<>();
		for (final Object o : doc.selectNodes("//dc:identifier")) {
			final String url = ((Node) o).getText().trim();
			if (url.startsWith("http")) {
				final Instance instance = new Instance();
				instance.setUrl(url);
				instance.setInstancetype(prepareQualifier(doc, "//dr:CobjCategory", "dnet:publication_resource", "dnet:publication_resource"));
				instance.setCollectedfrom(collectedfrom);
				instance.setHostedby(hostedby);
				instance.setDateofacceptance(field(doc.valueOf("//oaf:dateAccepted"), info));
				instance.setDistributionlocation(doc.valueOf("//oaf:distributionlocation"));
				instance.setAccessright(prepareQualifier(doc, "//oaf:accessrights", "dnet:access_modes", "dnet:access_modes"));
				instance.setLicense(field(doc.valueOf("//oaf:license"), info));
				res.add(instance);
			}
		}
		return res;
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

	// SOFTWARES

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

	// DATASETS
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

	// OTHER PRODUCTS

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

}
