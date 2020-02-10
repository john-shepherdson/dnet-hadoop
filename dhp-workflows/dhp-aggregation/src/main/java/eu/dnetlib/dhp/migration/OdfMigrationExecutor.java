package eu.dnetlib.dhp.migration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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

public class OdfMigrationExecutor extends AbstractMongoExecutor {

	private static final Log log = LogFactory.getLog(OdfMigrationExecutor.class);

	public OdfMigrationExecutor(final String hdfsPath, final String hdfsNameNode, final String hdfsUser, final String mongoBaseUrl, final String mongoDb,
			final String dbUrl, final String dbUser,
			final String dbPassword) throws Exception {
		super(hdfsPath, hdfsNameNode, hdfsUser, mongoBaseUrl, mongoDb, dbUrl, dbUser, dbPassword);
	}

	@Override
	protected void registerNamespaces(final Map<String, String> nsContext) {
		nsContext.put("dc", "http://datacite.org/schema/kernel-3");
	}

	@Override
	protected List<StructuredProperty> prepareTitles(final Document doc, final DataInfo info) {
		return prepareListStructProps(doc, "//dc:title", MAIN_TITLE_QUALIFIER, info);
	}

	@Override
	protected List<Author> prepareAuthors(final Document doc, final DataInfo info) {
		final List<Author> res = new ArrayList<>();
		int pos = 1;
		for (final Object o : doc.selectNodes("//dc:creator")) {
			final Node n = (Node) o;
			final Author author = new Author();
			author.setFullname(n.valueOf("./dc:creatorName"));
			author.setName(n.valueOf("./dc:givenName"));
			author.setSurname(n.valueOf("./dc:familyName"));
			author.setAffiliation(prepareListFields(doc, "./dc:affiliation", info));
			author.setPid(preparePids(doc, info));
			author.setRank(pos++);
			res.add(author);
		}
		return res;
	}

	private List<StructuredProperty> preparePids(final Document doc, final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : doc.selectNodes("./dc:nameIdentifier")) {
			res.add(structuredProperty(((Node) o).getText(), prepareQualifier((Node) o, "./@nameIdentifierScheme", "dnet:pid_types", "dnet:pid_types"), info));
		}
		return res;
	}

	@Override
	protected List<Instance> prepareInstances(final Document doc, final DataInfo info, final KeyValue collectedfrom, final KeyValue hostedby) {
		final List<Instance> res = new ArrayList<>();
		for (final Object o : doc.selectNodes("//dc:alternateIdentifier[@alternateIdentifierType='URL']")) {
			final Instance instance = new Instance();
			instance.setUrl(((Node) o).getText().trim());
			instance.setInstancetype(prepareQualifier(doc, "//dr:CobjCategory", "dnet:publication_resource", "dnet:publication_resource"));
			instance.setCollectedfrom(collectedfrom);
			instance.setHostedby(hostedby);
			instance.setDateofacceptance(field(doc.valueOf("//oaf:dateAccepted"), info));
			instance.setDistributionlocation(doc.valueOf("//oaf:distributionlocation"));
			instance.setAccessright(prepareQualifier(doc, "//oaf:accessrights", "dnet:access_modes", "dnet:access_modes"));
			instance.setLicense(field(doc.valueOf("//oaf:license"), info));
			res.add(instance);
		}
		return res;
	}

	@Override
	protected List<Field<String>> prepareSources(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<StructuredProperty> prepareRelevantDates(final Document doc, final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : doc.selectNodes("//dc:date")) {
			final String dateType = ((Node) o).valueOf("@dateType");
			if (StringUtils.isBlank(dateType) && !dateType.equalsIgnoreCase("Accepted") && !dateType.equalsIgnoreCase("Issued")
					&& !dateType.equalsIgnoreCase("Updated") && !dateType.equalsIgnoreCase("Available")) {
				res.add(structuredProperty(((Node) o).getText(), "UNKNOWN", "UNKNOWN", "dnet:dataCite_date", "dnet:dataCite_date", info));
			}
		}
		return res;
	}

	@Override
	protected List<Field<String>> prepareCoverages(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<Field<String>> prepareContributors(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:contributorName", info);
	}

	@Override
	protected List<Field<String>> prepareFormats(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:format", info);
	}

	@Override
	protected Field<String> preparePublisher(final Document doc, final DataInfo info) {
		return prepareField(doc, "//dc:publisher", info);
	}

	@Override
	protected List<Field<String>> prepareDescriptions(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:description[@descriptionType='Abstract']", info);
	}

	@Override
	protected List<StructuredProperty> prepareSubjects(final Document doc, final DataInfo info) {
		return prepareListStructProps(doc, "//dc:subject", info);
	}

	@Override
	protected Qualifier prepareLanguages(final Document doc) {
		return prepareQualifier(doc, "//dc:language", "dnet:languages", "dnet:languages");
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductTools(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactGroups(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:contributor[@contributorType='ContactGroup']/dc:contributorName", info);
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactPersons(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:contributor[@contributorType='ContactPerson']/dc:contributorName", info);
	}

	@Override
	protected Qualifier prepareSoftwareProgrammingLanguage(final Document doc, final DataInfo info) {
		return prepareQualifier(doc, "//dc:format", "dnet:programming_languages", "dnet:programming_languages");
	}

	@Override
	protected Field<String> prepareSoftwareCodeRepositoryUrl(final Document doc, final DataInfo info) {
		return null; // Not present in ODF ???
	}

	@Override
	protected List<StructuredProperty> prepareSoftwareLicenses(final Document doc, final DataInfo info) {
		return new ArrayList<>();  // Not present in ODF ???
	}

	@Override
	protected List<Field<String>> prepareSoftwareDocumentationUrls(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:relatedIdentifier[@relatedIdentifierType='URL' and @relationType='IsDocumentedBy']", info);
	}

	// DATASETS

	@Override
	protected List<GeoLocation> prepareDatasetGeoLocations(final Document doc, final DataInfo info) {
		final List<GeoLocation> res = new ArrayList<>();

		for (final Object o : doc.selectNodes("//dc:geoLocation")) {
			final GeoLocation loc = new GeoLocation();
			loc.setBox(((Node) o).valueOf("./dc:geoLocationBox"));
			loc.setPlace(((Node) o).valueOf("./dc:geoLocationPlace"));
			loc.setPoint(((Node) o).valueOf("./dc:geoLocationPoint"));
			res.add(loc);
		}
		return res;
	}

	@Override
	protected Field<String> prepareDatasetMetadataVersionNumber(final Document doc, final DataInfo info) {
		return null;		// Not present in ODF ???
	}

	@Override
	protected Field<String> prepareDatasetLastMetadataUpdate(final Document doc, final DataInfo info) {
		return prepareField(doc, "//dc:date[@dateType='Updated']", info);
	}

	@Override
	protected Field<String> prepareDatasetVersion(final Document doc, final DataInfo info) {
		return prepareField(doc, "//dc:version", info);
	}

	@Override
	protected Field<String> prepareDatasetSize(final Document doc, final DataInfo info) {
		return prepareField(doc, "//dc:size", info);
	}

	@Override
	protected Field<String> prepareDatasetDevice(final Document doc, final DataInfo info) {
		return null; // Not present in ODF ???
	}

	@Override
	protected Field<String> prepareDatasetStorageDate(final Document doc, final DataInfo info) {
		return prepareField(doc, "//dc:date[@dateType='Issued']", info);
	}

	@Override
	protected List<Oaf> addOtherResultRels(final Document doc, final KeyValue collectedFrom, final DataInfo info, final long lastUpdateTimestamp) {

		final String docId = createOpenaireId(50, doc.valueOf("//dri:objIdentifier"));

		final List<Oaf> res = new ArrayList<>();

		for (final Object o : doc.selectNodes("//*[local-name() = 'resource']//*[local-name()='relatedIdentifier' and ./@relatedIdentifierType='OPENAIRE']")) {
			final String otherId = createOpenaireId(50, ((Node) o).getText());
			final String type = ((Node) o).valueOf("@relationType");

			if (type.equals("IsSupplementTo")) {
				res.add(prepareOtherResultRel(collectedFrom, info, lastUpdateTimestamp, docId, otherId, "supplement", "isSupplementTo"));
				res.add(prepareOtherResultRel(collectedFrom, info, lastUpdateTimestamp, otherId, docId, "supplement", "isSupplementedBy"));
			} else if (type.equals("IsPartOf")) {
				res.add(prepareOtherResultRel(collectedFrom, info, lastUpdateTimestamp, docId, otherId, "part", "IsPartOf"));
				res.add(prepareOtherResultRel(collectedFrom, info, lastUpdateTimestamp, otherId, docId, "part", "HasParts"));
			} else {}
		}
		return res;
	}

	private Relation prepareOtherResultRel(final KeyValue collectedFrom,
			final DataInfo info,
			final long lastUpdateTimestamp,
			final String source,
			final String target,
			final String subRelType,
			final String relClass) {
		final Relation r = new Relation();
		r.setRelType("resultResult");
		r.setSubRelType(subRelType);
		r.setRelClass(relClass);
		r.setSource(source);
		r.setTarget(target);
		r.setCollectedFrom(Arrays.asList(collectedFrom));
		r.setDataInfo(info);
		r.setLastupdatetimestamp(lastUpdateTimestamp);
		return r;
	}

	@Override
	protected Qualifier prepareResourceType(final Document doc, final DataInfo info) {
		return prepareQualifier(doc, "//*[local-name() = 'resource']//*[local-name() = 'resourceType']", "dnet:dataCite_resource", "dnet:dataCite_resource");
	}

}
