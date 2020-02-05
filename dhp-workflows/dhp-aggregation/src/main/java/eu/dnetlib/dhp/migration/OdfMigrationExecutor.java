package eu.dnetlib.dhp.migration;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.GeoLocation;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class OdfMigrationExecutor extends AbstractMongoExecutor {

	private static final Log log = LogFactory.getLog(OdfMigrationExecutor.class);

	public OdfMigrationExecutor(final String hdfsPath, final String hdfsNameNode, final String hdfsUser, final String mongoBaseUrl, final String mongoDb,
			final String dbUrl, final String dbUser,
			final String dbPassword) throws Exception {
		super(hdfsPath, hdfsNameNode, hdfsUser, mongoBaseUrl, mongoDb, dbUrl, dbUser, dbPassword);
	}

	@Override
	protected void registerNamespaces(final Map<String, String> nsContext) {
		// TODO Auto-generated method stub

	}

	@Override
	protected List<Instance> prepareInstances(final Document doc, final DataInfo info, final KeyValue collectedfrom, final KeyValue hostedby) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<Field<String>> prepareSources(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
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
	protected List<Field<String>> prepareCoverages(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<Field<String>> prepareContributors(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<Field<String>> prepareFormats(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Field<String> preparePublisher(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<Field<String>> prepareDescriptions(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<StructuredProperty> prepareTitles(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<StructuredProperty> prepareSubjects(final Document doc, final DataInfo info) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Qualifier prepareLanguages(final Document doc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<Author> prepareAuthors(final Document doc, final DataInfo info) {
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

	@Override
	protected void addRelations(final List<Oaf> oafs,
			final Document doc,
			final String type,
			final KeyValue collectedFrom,
			final DataInfo info,
			final long lastUpdateTimestamp) {
		// TODO Auto-generated method stub

	}

}
