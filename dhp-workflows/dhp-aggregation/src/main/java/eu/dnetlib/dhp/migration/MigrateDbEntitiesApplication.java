package eu.dnetlib.dhp.migration;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class MigrateDbEntitiesApplication extends AbstractMigrateApplication implements Closeable {

	private static final Log log = LogFactory.getLog(MigrateDbEntitiesApplication.class);

	private final DbClient dbClient;

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils.toString(MigrateDbEntitiesApplication.class.getResourceAsStream("/eu/dnetlib/dhp/migration/migrate_db_entities_parameters.json")));

		parser.parseArgument(args);

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");

		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("namenode");
		final String hdfsUser = parser.get("hdfsUser");

		try (final MigrateDbEntitiesApplication smdbe = new MigrateDbEntitiesApplication(hdfsPath, hdfsNameNode, hdfsUser, dbUrl, dbUser, dbPassword)) {
			smdbe.execute("queryDatasources.sql", smdbe::processDatasource);
			smdbe.execute("queryProjects.sql", smdbe::processProject);
			smdbe.execute("queryOrganizations.sql", smdbe::processOrganization);
			smdbe.execute("queryDatasourceOrganization.sql", smdbe::processDatasourceOrganization);
			smdbe.execute("queryProjectOrganization.sql", smdbe::processProjectOrganization);
		}

	}

	public MigrateDbEntitiesApplication(final String hdfsPath, final String hdfsNameNode, final String hdfsUser, final String dbUrl, final String dbUser,
			final String dbPassword) throws Exception {
		super(hdfsPath, hdfsNameNode, hdfsUser);
		this.dbClient = new DbClient(dbUrl, dbUser, dbPassword);
	}

	public void execute(final String sqlFile, final Consumer<ResultSet> consumer) throws Exception {
		final String sql = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/migration/sql/" + sqlFile));
		dbClient.processResults(sql, consumer);
	}

	public void processDatasource(final ResultSet rs) {
		try {

			final DataInfo info = MigrationUtils.dataInfo(null, null, null, null, null, null); // TODO

			final Datasource ds = new Datasource();

			ds.setId(MigrationUtils.createOpenaireId("10", rs.getString("datasourceid")));
			ds.setOriginalId(Arrays.asList(rs.getString("datasourceid")));
			ds.setCollectedfrom(MigrationUtils.listKeyValues(rs.getString("collectedfromid"), rs.getString("collectedfromname")));
			ds.setPid(null); // List<StructuredProperty> // TODO
			ds.setDateofcollection(rs.getDate("dateofcollection").toString());
			ds.setDateoftransformation(null);  // TODO
			ds.setExtraInfo(null); // TODO
			ds.setOaiprovenance(null); // TODO

			ds.setDatasourcetype(null); // Qualifier datasourcetype) {
			ds.setOpenairecompatibility(null); // Qualifier openairecompatibility) {
			ds.setOfficialname(MigrationUtils.field(rs.getString("officialname"), info));
			ds.setEnglishname(MigrationUtils.field(rs.getString("englishname"), info));
			ds.setWebsiteurl(MigrationUtils.field(rs.getString("websiteurl"), info));
			ds.setLogourl(MigrationUtils.field(rs.getString("logourl"), info));
			ds.setContactemail(MigrationUtils.field(rs.getString("contactemail"), info));
			ds.setNamespaceprefix(MigrationUtils.field(rs.getString("namespaceprefix"), info));
			ds.setLatitude(MigrationUtils.field(Double.toString(rs.getDouble("latitude")), info));
			ds.setLongitude(MigrationUtils.field(Double.toString(rs.getDouble("longitude")), info));
			ds.setDateofvalidation(MigrationUtils.field(rs.getDate("dateofvalidation").toString(), info));
			ds.setDescription(MigrationUtils.field(rs.getString("description"), info));
			ds.setSubjects(null); // List<StructuredProperty> subjects) {
			ds.setOdnumberofitems(MigrationUtils.field(Double.toString(rs.getInt("odnumberofitems")), info));
			ds.setOdnumberofitemsdate(MigrationUtils.field(rs.getDate("odnumberofitemsdate").toString(), info));
			ds.setOdpolicies(MigrationUtils.field(rs.getString("odpolicies"), info));
			ds.setOdlanguages(MigrationUtils.listFields(info, rs.getArray("odlanguages")));
			ds.setOdcontenttypes(MigrationUtils.listFields(info, rs.getArray("odcontenttypes")));
			ds.setAccessinfopackage(MigrationUtils.listFields(info, rs.getArray("accessinfopackage")));
			ds.setReleasestartdate(MigrationUtils.field(rs.getDate("releasestartdate").toString(), info));
			ds.setReleaseenddate(MigrationUtils.field(rs.getDate("releaseenddate").toString(), info));
			ds.setMissionstatementurl(MigrationUtils.field(rs.getString("missionstatementurl"), info));
			ds.setDataprovider(MigrationUtils.field(rs.getBoolean("dataprovider"), info));
			ds.setServiceprovider(MigrationUtils.field(rs.getBoolean("serviceprovider"), info));
			ds.setDatabaseaccesstype(MigrationUtils.field(rs.getString("databaseaccesstype"), info));
			ds.setDatauploadtype(MigrationUtils.field(rs.getString("datauploadtype"), info));
			ds.setDatabaseaccessrestriction(MigrationUtils.field(rs.getString("databaseaccessrestriction"), info));
			ds.setDatauploadrestriction(MigrationUtils.field(rs.getString("datauploadrestriction"), info));
			ds.setVersioning(MigrationUtils.field(rs.getBoolean("versioning"), info));
			ds.setCitationguidelineurl(MigrationUtils.field(rs.getString("citationguidelineurl"), info));
			ds.setQualitymanagementkind(MigrationUtils.field(rs.getString("qualitymanagementkind"), info));
			ds.setPidsystems(MigrationUtils.field(rs.getString("pidsystems"), info));
			ds.setCertificates(MigrationUtils.field(rs.getString("certificates"), info));
			ds.setPolicies(null); // List<KeyValue> // TODO
			ds.setJournal(null); // Journal // TODO

			// rs.getString("datasourceid");
			rs.getArray("identities");
			// rs.getString("officialname");
			// rs.getString("englishname");
			// rs.getString("contactemail");
			rs.getString("openairecompatibility");  // COMPLEX ...@@@...
			// rs.getString("websiteurl");
			// rs.getString("logourl");
			// rs.getArray("accessinfopackage");
			// rs.getDouble("latitude");
			// rs.getDouble("longitude");
			// rs.getString("namespaceprefix");
			// rs.getInt("odnumberofitems"); // NULL
			// rs.getDate("odnumberofitemsdate"); // NULL
			rs.getArray("subjects");
			// rs.getString("description");
			// rs.getString("odpolicies"); // NULL
			// rs.getArray("odlanguages");
			// rs.getArray("odcontenttypes");
			rs.getBoolean("inferred");  // false
			rs.getBoolean("deletedbyinference");// false
			rs.getDouble("trust");  // 0.9
			rs.getString("inferenceprovenance"); // NULL
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
			rs.getArray("policies");
			// rs.getString("collectedfromid");
			// rs.getString("collectedfromname");
			rs.getString("datasourcetype");  // COMPLEX XXX@@@@....
			rs.getString("provenanceaction"); // 'sysimport:crosswalk:entityregistry@@@sysimport:crosswalk:entityregistry@@@dnet:provenance_actions@@@dnet:provenance_actions'
												 // AS provenanceaction,
			rs.getString("journal");  // CONCAT(d.issn, '@@@', d.eissn, '@@@', d.lissn) AS journal

			emitOaf(ds);
		} catch (final Exception e) {
			// TODO: handle exception
		}
	}

	public void processProject(final ResultSet rs) {
		try {

			final DataInfo info = MigrationUtils.dataInfo(null, null, null, null, null, null); // TODO

			final Project p = new Project();

			p.setId(MigrationUtils.createOpenaireId("40", rs.getString("projectid")));
			p.setOriginalId(Arrays.asList(rs.getString("projectid")));
			p.setCollectedfrom(MigrationUtils.listKeyValues(rs.getString("collectedfromid"), rs.getString("collectedfromname")));
			p.setPid(null); // List<StructuredProperty> // TODO

			p.setDateofcollection(rs.getDate("dateofcollection").toString());
			p.setDateoftransformation(rs.getDate("dateoftransformation").toString());
			p.setExtraInfo(null); // List<ExtraInfo> //TODO
			p.setOaiprovenance(null); // OAIProvenance /TODO

			p.setWebsiteurl(MigrationUtils.field(rs.getString("websiteurl"), info));
			p.setCode(MigrationUtils.field(rs.getString("code"), info));
			p.setAcronym(MigrationUtils.field(rs.getString("acronym"), info));
			p.setTitle(MigrationUtils.field(rs.getString("title"), info));
			p.setStartdate(MigrationUtils.field(rs.getDate("startdate").toString(), info));
			p.setEnddate(MigrationUtils.field(rs.getDate("enddate").toString(), info));
			p.setCallidentifier(MigrationUtils.field(rs.getString("callidentifier"), info));
			p.setKeywords(MigrationUtils.field(rs.getString("keywords"), info));
			p.setDuration(MigrationUtils.field(Integer.toString(rs.getInt("duration")), info));
			p.setEcsc39(MigrationUtils.field(Boolean.toString(rs.getBoolean("ecsc39")), info));
			p.setOamandatepublications(MigrationUtils.field(Boolean.toString(rs.getBoolean("oamandatepublications")), info));
			p.setEcarticle29_3(MigrationUtils.field(Boolean.toString(rs.getBoolean("ecarticle29_3")), info));
			p.setSubjects(null); // List<StructuredProperty> //TODO
			p.setFundingtree(null); // List<Field<String>> //TODO
			p.setContracttype(null); // Qualifier //TODO
			p.setOptional1(MigrationUtils.field(rs.getString("optional1"), info));
			p.setOptional2(MigrationUtils.field(rs.getString("optional2"), info));
			p.setJsonextrainfo(MigrationUtils.field(rs.getString("jsonextrainfo"), info));
			p.setContactfullname(MigrationUtils.field(rs.getString("contactfullname"), info));
			p.setContactfax(MigrationUtils.field(rs.getString("contactfax"), info));
			p.setContactphone(MigrationUtils.field(rs.getString("contactphone"), info));
			p.setContactemail(MigrationUtils.field(rs.getString("contactemail"), info));
			p.setSummary(MigrationUtils.field(rs.getString("summary"), info));
			p.setCurrency(MigrationUtils.field(rs.getString("currency"), info));
			p.setTotalcost(new Float(rs.getDouble("totalcost")));
			p.setFundedamount(new Float(rs.getDouble("fundedamount")));

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
			rs.getBoolean("inferred");
			rs.getBoolean("deletedbyinference");
			rs.getDouble("trust");
			rs.getString("inferenceprovenance");
			// rs.getString("optional1");
			// rs.getString("optional2");
			rs.getString("jsonextrainfo");
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
			rs.getString("contracttype");     // COMPLEX
			rs.getString("provenanceaction"); // COMPLEX
			rs.getArray("pid");
			rs.getArray("subjects");
			rs.getArray("fundingtree");

			emitOaf(p);

		} catch (final Exception e) {
			// TODO: handle exception
		}
	}

	public void processOrganization(final ResultSet rs) {
		try {

			final DataInfo info = MigrationUtils.dataInfo(null, null, null, null, null, null); // TODO

			final Organization o = new Organization();

			o.setId(MigrationUtils.createOpenaireId("20", rs.getString("organizationid"))); // String id) {
			o.setOriginalId(Arrays.asList(rs.getString("organizationid")));
			o.setCollectedfrom(MigrationUtils.listKeyValues(rs.getString("collectedfromid"), rs.getString("collectedfromname")));
			o.setPid(null); // List<StructuredProperty> // TODO
			o.setDateofcollection(rs.getDate("dateofcollection").toString());
			o.setDateoftransformation(rs.getDate("dateoftransformation").toString());
			o.setExtraInfo(null); // List<ExtraInfo> // TODO
			o.setOaiprovenance(null); // OAIProvenance // TODO
			o.setLegalshortname(MigrationUtils.field("legalshortname", info));
			o.setLegalname(MigrationUtils.field("legalname", info));
			o.setAlternativeNames(null); // List<Field<String>> //TODO
			o.setWebsiteurl(MigrationUtils.field("websiteurl", info));
			o.setLogourl(MigrationUtils.field("logourl", info));
			o.setEclegalbody(MigrationUtils.field(Boolean.toString(rs.getBoolean("eclegalbody")), info));
			o.setEclegalperson(MigrationUtils.field(Boolean.toString(rs.getBoolean("eclegalperson")), info));
			o.setEcnonprofit(MigrationUtils.field(Boolean.toString(rs.getBoolean("ecnonprofit")), info));
			o.setEcresearchorganization(MigrationUtils.field(Boolean.toString(rs.getBoolean("ecresearchorganization")), info));
			o.setEchighereducation(MigrationUtils.field(Boolean.toString(rs.getBoolean("echighereducation")), info));
			o.setEcinternationalorganizationeurinterests(MigrationUtils
					.field(Boolean.toString(rs.getBoolean("ecinternationalorganizationeurinterests")), info));
			o.setEcinternationalorganization(MigrationUtils.field(Boolean.toString(rs.getBoolean("ecinternationalorganization")), info));
			o.setEcenterprise(MigrationUtils.field(Boolean.toString(rs.getBoolean("ecenterprise")), info));
			o.setEcsmevalidated(MigrationUtils.field(Boolean.toString(rs.getBoolean("ecsmevalidated")), info));
			o.setEcnutscode(MigrationUtils.field(Boolean.toString(rs.getBoolean("ecnutscode")), info));
			o.setCountry(null); // Qualifier country) {

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
			rs.getDate("dateofcollection");
			rs.getDate("dateoftransformation");
			rs.getBoolean("inferred");
			rs.getBoolean("deletedbyinference");
			rs.getDouble("trust");
			rs.getString("inferenceprovenance");
			// rs.getString("collectedfromid");
			// rs.getString("collectedfromname");
			rs.getString("country");
			rs.getString("provenanceaction");
			rs.getArray("pid");

			emitOaf(o);
		} catch (final Exception e) {
			// TODO: handle exception
		}
	}

	public void processDatasourceOrganization(final ResultSet rs) {

		try {
			final Relation r = new Relation();

			r.setRelType(null); // TODO
			r.setSubRelType(null); // TODO
			r.setRelClass(null); // TODO
			r.setSource(null); // TODO
			r.setTarget(null); // TODO
			r.setCollectedFrom(MigrationUtils.listKeyValues("", ""));

			rs.getString("datasource");
			rs.getString("organization");
			rs.getDate("startdate");  // NULL
			rs.getDate("enddate");    // NULL
			rs.getBoolean("inferred"); // false
			rs.getBoolean("deletedbyinference"); // false
			rs.getDouble("trust");                // 0.9
			rs.getString("inferenceprovenance"); // NULL
			rs.getString("semantics");  // 'providedBy@@@provided
										  // by@@@dnet:datasources_organizations_typologies@@@dnet:datasources_organizations_typologies' AS
										  // semantics,
			rs.getString("provenanceaction"); // d.provenanceaction || '@@@' || d.provenanceaction ||
												 // '@@@dnet:provenanceActions@@@dnet:provenanceActions' AS provenanceaction

			emitOaf(r);
		} catch (final Exception e) {
			// TODO: handle exception
		}
	}

	public void processProjectOrganization(final ResultSet rs) {
		try {
			final Relation r = new Relation();

			r.setRelType(null); // TODO
			r.setSubRelType(null); // TODO
			r.setRelClass(null); // TODO
			r.setSource(null); // TODO
			r.setTarget(null); // TODO
			r.setCollectedFrom(null);

			rs.getString("project");
			rs.getString("resporganization");
			rs.getInt("participantnumber");
			rs.getDouble("contribution");
			rs.getDate("startdate");// null
			rs.getDate("enddate");// null
			rs.getBoolean("inferred");// false
			rs.getBoolean("deletedbyinference"); // false
			rs.getDouble("trust");
			rs.getString("inferenceprovenance"); // NULL
			rs.getString("semantics"); // po.semanticclass || '@@@' || po.semanticclass ||
										 // '@@@dnet:project_organization_relations@@@dnet:project_organization_relations' AS semantics,
			rs.getString("provenanceaction"); // 'sysimport:crosswalk:entityregistry@@@sysimport:crosswalk:entityregistry@@@dnet:provenance_actions@@@dnet:provenance_actions'
												 // AS provenanceaction
			emitOaf(r);
		} catch (final Exception e) {
			// TODO: handle exception
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		dbClient.close();
	}

}
