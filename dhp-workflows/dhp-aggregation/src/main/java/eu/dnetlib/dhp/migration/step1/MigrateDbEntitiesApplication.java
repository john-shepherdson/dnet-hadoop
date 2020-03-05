package eu.dnetlib.dhp.migration.step1;

import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.asString;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.createOpenaireId;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.dataInfo;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.field;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.journal;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.listFields;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.listKeyValues;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.qualifier;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.structuredProperty;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.migration.utils.AbstractMigrationApplication;
import eu.dnetlib.dhp.migration.utils.DbClient;
import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class MigrateDbEntitiesApplication extends AbstractMigrationApplication implements Closeable {

	private static final Qualifier ENTITYREGISTRY_PROVENANCE_ACTION =
			qualifier("sysimport:crosswalk:entityregistry", "sysimport:crosswalk:entityregistry", "dnet:provenance_actions", "dnet:provenance_actions");

	private static final Log log = LogFactory.getLog(MigrateDbEntitiesApplication.class);

	private final DbClient dbClient;

	private final long lastUpdateTimestamp;

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils.toString(MigrateDbEntitiesApplication.class.getResourceAsStream("/eu/dnetlib/dhp/migration/migrate_db_entities_parameters.json")));

		parser.parseArgument(args);

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");

		final String hdfsPath = parser.get("hdfsPath");

		final boolean processClaims = parser.get("action") != null && parser.get("action").equalsIgnoreCase("claims");

		try (final MigrateDbEntitiesApplication smdbe = new MigrateDbEntitiesApplication(hdfsPath, dbUrl, dbUser, dbPassword)) {
			if (processClaims) {
				log.info("Processing claims...");
				smdbe.execute("queryClaims.sql", smdbe::processClaims);
			} else {
				log.info("Processing datasources...");
				smdbe.execute("queryDatasources.sql", smdbe::processDatasource);

				log.info("Processing projects...");
				smdbe.execute("queryProjects.sql", smdbe::processProject);

				log.info("Processing orgs...");
				smdbe.execute("queryOrganizations.sql", smdbe::processOrganization);

				log.info("Processing relations ds <-> orgs ...");
				smdbe.execute("queryDatasourceOrganization.sql", smdbe::processDatasourceOrganization);

				log.info("Processing projects <-> orgs ...");
				smdbe.execute("queryProjectOrganization.sql", smdbe::processProjectOrganization);
			}
			log.info("All done.");
		}
	}

	public MigrateDbEntitiesApplication(final String hdfsPath, final String dbUrl, final String dbUser,
			final String dbPassword) throws Exception {
		super(hdfsPath);
		this.dbClient = new DbClient(dbUrl, dbUser, dbPassword);
		this.lastUpdateTimestamp = new Date().getTime();
	}

	public void execute(final String sqlFile, final Consumer<ResultSet> consumer) throws Exception {
		final String sql = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/migration/sql/" + sqlFile));
		dbClient.processResults(sql, consumer);
	}

	public void processDatasource(final ResultSet rs) {

		try {

			final DataInfo info = prepareDataInfo(rs);

			final Datasource ds = new Datasource();

			ds.setId(createOpenaireId(10, rs.getString("datasourceid"), true));
			ds.setOriginalId(Arrays.asList(rs.getString("datasourceid")));
			ds.setCollectedfrom(listKeyValues(rs.getString("collectedfromid"), rs.getString("collectedfromname")));
			ds.setPid(new ArrayList<>());
			ds.setDateofcollection(asString(rs.getDate("dateofcollection")));
			ds.setDateoftransformation(null);   // Value not returned by the SQL query
			ds.setExtraInfo(new ArrayList<>());  // Values not present in the DB
			ds.setOaiprovenance(null); // Values not present in the DB
			ds.setDatasourcetype(prepareQualifierSplitting(rs.getString("datasourcetype")));
			ds.setOpenairecompatibility(prepareQualifierSplitting(rs.getString("openairecompatibility")));
			ds.setOfficialname(field(rs.getString("officialname"), info));
			ds.setEnglishname(field(rs.getString("englishname"), info));
			ds.setWebsiteurl(field(rs.getString("websiteurl"), info));
			ds.setLogourl(field(rs.getString("logourl"), info));
			ds.setContactemail(field(rs.getString("contactemail"), info));
			ds.setNamespaceprefix(field(rs.getString("namespaceprefix"), info));
			ds.setLatitude(field(Double.toString(rs.getDouble("latitude")), info));
			ds.setLongitude(field(Double.toString(rs.getDouble("longitude")), info));
			ds.setDateofvalidation(field(asString(rs.getDate("dateofvalidation")), info));
			ds.setDescription(field(rs.getString("description"), info));
			ds.setSubjects(prepareListOfStructProps(rs.getArray("subjects"), info));
			ds.setOdnumberofitems(field(Double.toString(rs.getInt("odnumberofitems")), info));
			ds.setOdnumberofitemsdate(field(asString(rs.getDate("odnumberofitemsdate")), info));
			ds.setOdpolicies(field(rs.getString("odpolicies"), info));
			ds.setOdlanguages(prepareListFields(rs.getArray("odlanguages"), info));
			ds.setOdcontenttypes(prepareListFields(rs.getArray("odcontenttypes"), info));
			ds.setAccessinfopackage(prepareListFields(rs.getArray("accessinfopackage"), info));
			ds.setReleasestartdate(field(asString(rs.getDate("releasestartdate")), info));
			ds.setReleaseenddate(field(asString(rs.getDate("releaseenddate")), info));
			ds.setMissionstatementurl(field(rs.getString("missionstatementurl"), info));
			ds.setDataprovider(field(rs.getBoolean("dataprovider"), info));
			ds.setServiceprovider(field(rs.getBoolean("serviceprovider"), info));
			ds.setDatabaseaccesstype(field(rs.getString("databaseaccesstype"), info));
			ds.setDatauploadtype(field(rs.getString("datauploadtype"), info));
			ds.setDatabaseaccessrestriction(field(rs.getString("databaseaccessrestriction"), info));
			ds.setDatauploadrestriction(field(rs.getString("datauploadrestriction"), info));
			ds.setVersioning(field(rs.getBoolean("versioning"), info));
			ds.setCitationguidelineurl(field(rs.getString("citationguidelineurl"), info));
			ds.setQualitymanagementkind(field(rs.getString("qualitymanagementkind"), info));
			ds.setPidsystems(field(rs.getString("pidsystems"), info));
			ds.setCertificates(field(rs.getString("certificates"), info));
			ds.setPolicies(new ArrayList<>()); // The sql query returns an empty array
			ds.setJournal(prepareJournal(rs.getString("officialname"), rs.getString("journal"), info)); // Journal
			ds.setDataInfo(info);
			ds.setLastupdatetimestamp(lastUpdateTimestamp);

			// rs.getString("datasourceid");
			// rs.getArray("identities");
			// rs.getString("officialname");
			// rs.getString("englishname");
			// rs.getString("contactemail");
			// rs.getString("openairecompatibility"); // COMPLEX ...@@@...
			// rs.getString("websiteurl");
			// rs.getString("logourl");
			// rs.getArray("accessinfopackage");
			// rs.getDouble("latitude");
			// rs.getDouble("longitude");
			// rs.getString("namespaceprefix");
			// rs.getInt("odnumberofitems"); // NULL
			// rs.getDate("odnumberofitemsdate"); // NULL
			// rs.getArray("subjects");
			// rs.getString("description");
			// rs.getString("odpolicies"); // NULL
			// rs.getArray("odlanguages");
			// rs.getArray("odcontenttypes");
			// rs.getBoolean("inferred"); // false
			// rs.getBoolean("deletedbyinference");// false
			// rs.getDouble("trust"); // 0.9
			// rs.getString("inferenceprovenance"); // NULL
			// rs.getDate("dateofcollection");
			// rs.getDate("dateofvalidation");
			// rs.getDate("releasestartdate");
			// rs.getDate("releaseenddate");
			// rs.getString("missionstatementurl");
			// rs.getBoolean("dataprovider");
			// rs.getBoolean("serviceprovider");
			// rs.getString("databaseaccesstype");
			// rs.getString("datauploadtype");
			// rs.getString("databaseaccessrestriction");
			// rs.getString("datauploadrestriction");
			// rs.getBoolean("versioning");
			// rs.getString("citationguidelineurl");
			// rs.getString("qualitymanagementkind");
			// rs.getString("pidsystems");
			// rs.getString("certificates");
			// rs.getArray("policies");
			// rs.getString("collectedfromid");
			// rs.getString("collectedfromname");
			// rs.getString("datasourcetype"); // COMPLEX
			// rs.getString("provenanceaction"); //
			// 'sysimport:crosswalk:entityregistry@@@sysimport:crosswalk:entityregistry@@@dnet:provenance_actions@@@dnet:provenance_actions'
			// AS provenanceaction,
			// rs.getString("journal"); // CONCAT(d.issn, '@@@', d.eissn, '@@@', d.lissn) AS journal

			emitOaf(ds);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void processProject(final ResultSet rs) {
		try {

			final DataInfo info = prepareDataInfo(rs);

			final Project p = new Project();

			p.setId(createOpenaireId(40, rs.getString("projectid"), true));
			p.setOriginalId(Arrays.asList(rs.getString("projectid")));
			p.setCollectedfrom(listKeyValues(rs.getString("collectedfromid"), rs.getString("collectedfromname")));
			p.setPid(new ArrayList<>());
			p.setDateofcollection(asString(rs.getDate("dateofcollection")));
			p.setDateoftransformation(asString(rs.getDate("dateoftransformation")));
			p.setExtraInfo(new ArrayList<>());  // Values not present in the DB
			p.setOaiprovenance(null); // Values not present in the DB
			p.setWebsiteurl(field(rs.getString("websiteurl"), info));
			p.setCode(field(rs.getString("code"), info));
			p.setAcronym(field(rs.getString("acronym"), info));
			p.setTitle(field(rs.getString("title"), info));
			p.setStartdate(field(asString(rs.getDate("startdate")), info));
			p.setEnddate(field(asString(rs.getDate("enddate")), info));
			p.setCallidentifier(field(rs.getString("callidentifier"), info));
			p.setKeywords(field(rs.getString("keywords"), info));
			p.setDuration(field(Integer.toString(rs.getInt("duration")), info));
			p.setEcsc39(field(Boolean.toString(rs.getBoolean("ecsc39")), info));
			p.setOamandatepublications(field(Boolean.toString(rs.getBoolean("oamandatepublications")), info));
			p.setEcarticle29_3(field(Boolean.toString(rs.getBoolean("ecarticle29_3")), info));
			p.setSubjects(prepareListOfStructProps(rs.getArray("subjects"), info));
			p.setFundingtree(prepareListFields(rs.getArray("fundingtree"), info));
			p.setContracttype(prepareQualifierSplitting(rs.getString("contracttype")));
			p.setOptional1(field(rs.getString("optional1"), info));
			p.setOptional2(field(rs.getString("optional2"), info));
			p.setJsonextrainfo(field(rs.getString("jsonextrainfo"), info));
			p.setContactfullname(field(rs.getString("contactfullname"), info));
			p.setContactfax(field(rs.getString("contactfax"), info));
			p.setContactphone(field(rs.getString("contactphone"), info));
			p.setContactemail(field(rs.getString("contactemail"), info));
			p.setSummary(field(rs.getString("summary"), info));
			p.setCurrency(field(rs.getString("currency"), info));
			p.setTotalcost(new Float(rs.getDouble("totalcost")));
			p.setFundedamount(new Float(rs.getDouble("fundedamount")));
			p.setDataInfo(info);
			p.setLastupdatetimestamp(lastUpdateTimestamp);

			// rs.getString("projectid");
			// rs.getString("code");
			// rs.getString("websiteurl");
			// rs.getString("acronym");
			// rs.getString("title");
			// rs.getDate("startdate");
			// rs.getDate("enddate");
			// rs.getString("callidentifier");
			// rs.getString("keywords");
			// rs.getInt("duration");
			// rs.getBoolean("ecsc39");
			// rs.getBoolean("oamandatepublications");
			// rs.getBoolean("ecarticle29_3");
			// rs.getDate("dateofcollection");
			// rs.getDate("dateoftransformation");
			// rs.getBoolean("inferred");
			// rs.getBoolean("deletedbyinference");
			// rs.getDouble("trust");
			// rs.getString("inferenceprovenance");
			// rs.getString("optional1");
			// rs.getString("optional2");
			// rs.getString("jsonextrainfo");
			// rs.getString("contactfullname");
			// rs.getString("contactfax");
			// rs.getString("contactphone");
			// rs.getString("contactemail");
			// rs.getString("summary");
			// rs.getString("currency");
			// rs.getDouble("totalcost");
			// rs.getDouble("fundedamount");
			// rs.getString("collectedfromid");
			// rs.getString("collectedfromname");
			// rs.getString("contracttype"); // COMPLEX
			// rs.getString("provenanceaction"); // COMPLEX
			// rs.getArray("pid");
			// rs.getArray("subjects");
			// rs.getArray("fundingtree");

			emitOaf(p);

		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void processOrganization(final ResultSet rs) {

		try {

			final DataInfo info = prepareDataInfo(rs);

			final Organization o = new Organization();

			o.setId(createOpenaireId(20, rs.getString("organizationid"), true));
			o.setOriginalId(Arrays.asList(rs.getString("organizationid")));
			o.setCollectedfrom(listKeyValues(rs.getString("collectedfromid"), rs.getString("collectedfromname")));
			o.setPid(new ArrayList<>());
			o.setDateofcollection(asString(rs.getDate("dateofcollection")));
			o.setDateoftransformation(asString(rs.getDate("dateoftransformation")));
			o.setExtraInfo(new ArrayList<>());  // Values not present in the DB
			o.setOaiprovenance(null); // Values not present in the DB
			o.setLegalshortname(field("legalshortname", info));
			o.setLegalname(field("legalname", info));
			o.setAlternativeNames(new ArrayList<>());  // Values not returned by the SQL query
			o.setWebsiteurl(field("websiteurl", info));
			o.setLogourl(field("logourl", info));
			o.setEclegalbody(field(Boolean.toString(rs.getBoolean("eclegalbody")), info));
			o.setEclegalperson(field(Boolean.toString(rs.getBoolean("eclegalperson")), info));
			o.setEcnonprofit(field(Boolean.toString(rs.getBoolean("ecnonprofit")), info));
			o.setEcresearchorganization(field(Boolean.toString(rs.getBoolean("ecresearchorganization")), info));
			o.setEchighereducation(field(Boolean.toString(rs.getBoolean("echighereducation")), info));
			o.setEcinternationalorganizationeurinterests(field(Boolean.toString(rs.getBoolean("ecinternationalorganizationeurinterests")), info));
			o.setEcinternationalorganization(field(Boolean.toString(rs.getBoolean("ecinternationalorganization")), info));
			o.setEcenterprise(field(Boolean.toString(rs.getBoolean("ecenterprise")), info));
			o.setEcsmevalidated(field(Boolean.toString(rs.getBoolean("ecsmevalidated")), info));
			o.setEcnutscode(field(Boolean.toString(rs.getBoolean("ecnutscode")), info));
			o.setCountry(prepareQualifierSplitting(rs.getString("country")));
			o.setDataInfo(info);
			o.setLastupdatetimestamp(lastUpdateTimestamp);

			// rs.getString("organizationid");
			// rs.getString("legalshortname");
			// rs.getString("legalname");
			// rs.getString("websiteurl");
			// rs.getString("logourl");
			// rs.getBoolean("eclegalbody");
			// rs.getBoolean("eclegalperson");
			// rs.getBoolean("ecnonprofit");
			// rs.getBoolean("ecresearchorganization");
			// rs.getBoolean("echighereducation");
			// rs.getBoolean("ecinternationalorganizationeurinterests");
			// rs.getBoolean("ecinternationalorganization");
			// rs.getBoolean("ecenterprise");
			// rs.getBoolean("ecsmevalidated");
			// rs.getBoolean("ecnutscode");
			// rs.getDate("dateofcollection");
			// rs.getDate("dateoftransformation");
			// rs.getBoolean("inferred");
			// rs.getBoolean("deletedbyinference");
			// rs.getDouble("trust");
			// rs.getString("inferenceprovenance");
			// rs.getString("collectedfromid");
			// rs.getString("collectedfromname");
			// rs.getString("country");
			// rs.getString("provenanceaction");
			// rs.getArray("pid");

			emitOaf(o);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void processDatasourceOrganization(final ResultSet rs) {

		try {
			final DataInfo info = prepareDataInfo(rs);
			final String orgId = createOpenaireId(20, rs.getString("organization"), true);
			final String dsId = createOpenaireId(10, rs.getString("datasource"), true);
			final List<KeyValue> collectedFrom = listKeyValues(rs.getString("collectedfromid"), rs.getString("collectedfromname"));

			final Relation r1 = new Relation();
			r1.setRelType("datasourceOrganization");
			r1.setSubRelType("provision");
			r1.setRelClass("isProvidedBy");
			r1.setSource(dsId);
			r1.setTarget(orgId);
			r1.setCollectedFrom(collectedFrom);
			r1.setDataInfo(info);
			r1.setLastupdatetimestamp(lastUpdateTimestamp);
			emitOaf(r1);

			final Relation r2 = new Relation();
			r2.setRelType("datasourceOrganization");
			r2.setSubRelType("provision");
			r2.setRelClass("provides");
			r2.setSource(orgId);
			r2.setTarget(dsId);
			r2.setCollectedFrom(collectedFrom);
			r2.setDataInfo(info);
			r2.setLastupdatetimestamp(lastUpdateTimestamp);
			emitOaf(r2);

			// rs.getString("datasource");
			// rs.getString("organization");
			// rs.getDate("startdate"); // NULL
			// rs.getDate("enddate"); // NULL
			// rs.getBoolean("inferred"); // false
			// rs.getBoolean("deletedbyinference"); // false
			// rs.getDouble("trust"); // 0.9
			// rs.getString("inferenceprovenance"); // NULL
			// rs.getString("semantics"); // 'providedBy@@@provided
			// by@@@dnet:datasources_organizations_typologies@@@dnet:datasources_organizations_typologies' AS
			// semantics,
			// rs.getString("provenanceaction"); // d.provenanceaction || '@@@' || d.provenanceaction ||
			// '@@@dnet:provenanceActions@@@dnet:provenanceActions' AS provenanceaction

		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void processProjectOrganization(final ResultSet rs) {

		try {
			final DataInfo info = prepareDataInfo(rs);
			final String orgId = createOpenaireId(20, rs.getString("resporganization"), true);
			final String projectId = createOpenaireId(40, rs.getString("project"), true);
			final List<KeyValue> collectedFrom = listKeyValues(rs.getString("collectedfromid"), rs.getString("collectedfromname"));

			final Relation r1 = new Relation();
			r1.setRelType("projectOrganization");
			r1.setSubRelType("participation");
			r1.setRelClass("isParticipant");
			r1.setSource(projectId);
			r1.setTarget(orgId);
			r1.setCollectedFrom(collectedFrom);
			r1.setDataInfo(info);
			r1.setLastupdatetimestamp(lastUpdateTimestamp);
			emitOaf(r1);

			final Relation r2 = new Relation();
			r2.setRelType("projectOrganization");
			r2.setSubRelType("participation");
			r2.setRelClass("hasParticipant");
			r2.setSource(orgId);
			r2.setTarget(projectId);
			r2.setCollectedFrom(collectedFrom);
			r2.setDataInfo(info);
			r2.setLastupdatetimestamp(lastUpdateTimestamp);
			emitOaf(r2);

			// rs.getString("project");
			// rs.getString("resporganization");
			// rs.getInt("participantnumber");
			// rs.getDouble("contribution");
			// rs.getDate("startdate");// null
			// rs.getDate("enddate");// null
			// rs.getBoolean("inferred");// false
			// rs.getBoolean("deletedbyinference"); // false
			// rs.getDouble("trust");
			// rs.getString("inferenceprovenance"); // NULL
			// rs.getString("semantics"); // po.semanticclass || '@@@' || po.semanticclass ||
			// '@@@dnet:project_organization_relations@@@dnet:project_organization_relations' AS semantics,
			// rs.getString("provenanceaction"); //
			// 'sysimport:crosswalk:entityregistry@@@sysimport:crosswalk:entityregistry@@@dnet:provenance_actions@@@dnet:provenance_actions'
			// AS provenanceaction

		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void processClaims(final ResultSet rs) {

		final DataInfo info =
				dataInfo(false, null, false, false, qualifier("user:claim", "user:claim", "dnet:provenanceActions", "dnet:provenanceActions"), "0.9");

		try {

			if (rs.getString("source_type").equals("context")) {
				final Result r;

				if (rs.getString("target_type").equals("dataset")) {
					r = new Dataset();
				} else if (rs.getString("target_type").equals("software")) {
					r = new Software();
				} else if (rs.getString("target_type").equals("other")) {
					r = new OtherResearchProduct();
				} else {
					r = new Publication();
				}
				r.setId(createOpenaireId(50, rs.getString("target_id"), false));
				r.setLastupdatetimestamp(lastUpdateTimestamp);
				r.setContext(prepareContext(rs.getString("source_id"), info));
				r.setDataInfo(info);
				emitOaf(r);
			} else {
				final String sourceId = createOpenaireId(rs.getString("source_type"), rs.getString("source_id"), false);
				final String targetId = createOpenaireId(rs.getString("target_type"), rs.getString("target_id"), false);

				final Relation r1 = new Relation();
				final Relation r2 = new Relation();

				if (rs.getString("source_type").equals("project")) {
					r1.setRelType("resultProject");
					r1.setSubRelType("outcome");
					r1.setRelClass("produces");

					r2.setRelType("resultProject");
					r2.setSubRelType("outcome");
					r2.setRelClass("isProducedBy");
				} else {
					r1.setRelType("resultResult");
					r1.setSubRelType("relationship");
					r1.setRelClass("isRelatedTo");

					r2.setRelType("resultResult");
					r2.setSubRelType("relationship");
					r2.setRelClass("isRelatedTo");
				}

				r1.setSource(sourceId);
				r1.setTarget(targetId);
				r1.setDataInfo(info);
				r1.setLastupdatetimestamp(lastUpdateTimestamp);
				emitOaf(r1);

				r2.setSource(targetId);
				r2.setTarget(sourceId);
				r2.setDataInfo(info);
				r2.setLastupdatetimestamp(lastUpdateTimestamp);
				emitOaf(r2);

			}

		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private List<Context> prepareContext(final String id, final DataInfo dataInfo) {
		final Context context = new Context();
		context.setId(id);
		context.setDataInfo(Arrays.asList(dataInfo));
		return Arrays.asList(context);
	}

	private DataInfo prepareDataInfo(final ResultSet rs) throws SQLException {
		final Boolean deletedbyinference = rs.getBoolean("deletedbyinference");
		final String inferenceprovenance = rs.getString("inferenceprovenance");
		final Boolean inferred = rs.getBoolean("inferred");
		final String trust = rs.getString("trust");
		return dataInfo(deletedbyinference, inferenceprovenance, inferred, false, ENTITYREGISTRY_PROVENANCE_ACTION, trust);
	}

	private Qualifier prepareQualifierSplitting(final String s) {
		if (StringUtils.isBlank(s)) { return null; }
		final String[] arr = s.split("@@@");
		return arr.length == 4 ? qualifier(arr[0], arr[1], arr[2], arr[3]) : null;
	}

	private List<Field<String>> prepareListFields(final Array array, final DataInfo info) {
		try {
			return listFields(info, (String[]) array.getArray());
		} catch (final SQLException e) {
			throw new RuntimeException("Invalid SQL array", e);
		}
	}

	private StructuredProperty prepareStructProp(final String s, final DataInfo dataInfo) {
		if (StringUtils.isBlank(s)) { return null; }
		final String[] parts = s.split("###");
		if (parts.length == 2) {
			final String value = parts[0];
			final String[] arr = parts[1].split("@@@");
			if (arr.length == 4) { return structuredProperty(value, arr[0], arr[1], arr[2], arr[3], dataInfo); }
		}
		return null;
	}

	private List<StructuredProperty> prepareListOfStructProps(final Array array, final DataInfo dataInfo) throws SQLException {
		final List<StructuredProperty> res = new ArrayList<>();
		if (array != null) {
			for (final String s : (String[]) array.getArray()) {
				final StructuredProperty sp = prepareStructProp(s, dataInfo);
				if (sp != null) {
					res.add(sp);
				}
			}
		}

		return res;
	}

	private Journal prepareJournal(final String name, final String sj, final DataInfo info) {
		if (StringUtils.isNotBlank(sj)) {
			final String[] arr = sj.split("@@@");
			if (arr.length == 3) {
				final String issn = StringUtils.isNotBlank(arr[0]) ? arr[0] : null;
				final String eissn = StringUtils.isNotBlank(arr[1]) ? arr[1] : null;;
				final String lissn = StringUtils.isNotBlank(arr[2]) ? arr[2] : null;;
				if (issn != null || eissn != null
						|| lissn != null) { return journal(name, issn, eissn, eissn, null, null, null, null, null, null, null, info); }
			}
		}
		return null;
	}

	@Override
	public void close() throws IOException {
		super.close();
		dbClient.close();
	}

}
