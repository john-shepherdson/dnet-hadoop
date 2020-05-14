
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.asString;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.createOpenaireId;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.dataInfo;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.field;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.journal;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.listFields;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.listKeyValues;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.qualifier;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.structuredProperty;
import static eu.dnetlib.dhp.schema.common.ModelConstants.*;

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
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.DbClient;
import eu.dnetlib.dhp.oa.graph.raw.common.AbstractMigrationApplication;
import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Oaf;
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

	private static final Log log = LogFactory.getLog(MigrateDbEntitiesApplication.class);

	public static final String SOURCE_TYPE = "source_type";
	public static final String TARGET_TYPE = "target_type";

	private final DbClient dbClient;

	private final long lastUpdateTimestamp;

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					MigrateDbEntitiesApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/migrate_db_entities_parameters.json")));

		parser.parseArgument(args);

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");

		final String hdfsPath = parser.get("hdfsPath");

		final boolean processClaims = parser.get("action") != null && parser.get("action").equalsIgnoreCase("claims");

		try (final MigrateDbEntitiesApplication smdbe = new MigrateDbEntitiesApplication(hdfsPath, dbUrl, dbUser,
			dbPassword)) {
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

				log.info("Processing relationsNoRemoval ds <-> orgs ...");
				smdbe.execute("queryDatasourceOrganization.sql", smdbe::processDatasourceOrganization);

				log.info("Processing projects <-> orgs ...");
				smdbe.execute("queryProjectOrganization.sql", smdbe::processProjectOrganization);
			}
			log.info("All done.");
		}
	}

	protected MigrateDbEntitiesApplication() { // ONLY FOR UNIT TEST
		super();
		this.dbClient = null;
		this.lastUpdateTimestamp = new Date().getTime();
	}

	public MigrateDbEntitiesApplication(
		final String hdfsPath, final String dbUrl, final String dbUser, final String dbPassword)
		throws Exception {
		super(hdfsPath);
		this.dbClient = new DbClient(dbUrl, dbUser, dbPassword);
		this.lastUpdateTimestamp = new Date().getTime();
	}

	public void execute(final String sqlFile, final Function<ResultSet, List<Oaf>> producer)
		throws Exception {
		final String sql = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/sql/" + sqlFile));

		final Consumer<ResultSet> consumer = rs -> producer.apply(rs).forEach(oaf -> emitOaf(oaf));

		dbClient.processResults(sql, consumer);
	}

	public List<Oaf> processDatasource(final ResultSet rs) {
		try {
			final DataInfo info = prepareDataInfo(rs);

			final Datasource ds = new Datasource();

			ds.setId(createOpenaireId(10, rs.getString("datasourceid"), true));
			ds.setOriginalId(Arrays.asList(rs.getString("datasourceid")));
			ds
				.setCollectedfrom(
					listKeyValues(
						createOpenaireId(10, rs.getString("collectedfromid"), true),
						rs.getString("collectedfromname")));
			ds.setPid(new ArrayList<>());
			ds.setDateofcollection(asString(rs.getDate("dateofcollection")));
			ds.setDateoftransformation(null); // Value not returned by the SQL query
			ds.setExtraInfo(new ArrayList<>()); // Values not present in the DB
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
			ds
				.setJournal(prepareJournal(rs.getString("officialname"), rs.getString("journal"), info)); // Journal
			ds.setDataInfo(info);
			ds.setLastupdatetimestamp(lastUpdateTimestamp);

			return Arrays.asList(ds);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processProject(final ResultSet rs) {
		try {
			final DataInfo info = prepareDataInfo(rs);

			final Project p = new Project();

			p.setId(createOpenaireId(40, rs.getString("projectid"), true));
			p.setOriginalId(Arrays.asList(rs.getString("projectid")));
			p
				.setCollectedfrom(
					listKeyValues(
						createOpenaireId(10, rs.getString("collectedfromid"), true),
						rs.getString("collectedfromname")));
			p.setPid(new ArrayList<>());
			p.setDateofcollection(asString(rs.getDate("dateofcollection")));
			p.setDateoftransformation(asString(rs.getDate("dateoftransformation")));
			p.setExtraInfo(new ArrayList<>()); // Values not present in the DB
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
			p
				.setOamandatepublications(field(Boolean.toString(rs.getBoolean("oamandatepublications")), info));
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

			return Arrays.asList(p);

		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processOrganization(final ResultSet rs) {
		try {
			final DataInfo info = prepareDataInfo(rs);

			final Organization o = new Organization();

			o.setId(createOpenaireId(20, rs.getString("organizationid"), true));
			o.setOriginalId(Arrays.asList(rs.getString("organizationid")));
			o
				.setCollectedfrom(
					listKeyValues(
						createOpenaireId(10, rs.getString("collectedfromid"), true),
						rs.getString("collectedfromname")));
			o.setPid(new ArrayList<>());
			o.setDateofcollection(asString(rs.getDate("dateofcollection")));
			o.setDateoftransformation(asString(rs.getDate("dateoftransformation")));
			o.setExtraInfo(new ArrayList<>()); // Values not present in the DB
			o.setOaiprovenance(null); // Values not present in the DB
			o.setLegalshortname(field(rs.getString("legalshortname"), info));
			o.setLegalname(field(rs.getString("legalname"), info));
			o.setAlternativeNames(new ArrayList<>()); // Values not returned by the SQL query
			o.setWebsiteurl(field(rs.getString("websiteurl"), info));
			o.setLogourl(field(rs.getString("logourl"), info));
			o.setEclegalbody(field(Boolean.toString(rs.getBoolean("eclegalbody")), info));
			o.setEclegalperson(field(Boolean.toString(rs.getBoolean("eclegalperson")), info));
			o.setEcnonprofit(field(Boolean.toString(rs.getBoolean("ecnonprofit")), info));
			o
				.setEcresearchorganization(field(Boolean.toString(rs.getBoolean("ecresearchorganization")), info));
			o.setEchighereducation(field(Boolean.toString(rs.getBoolean("echighereducation")), info));
			o
				.setEcinternationalorganizationeurinterests(
					field(Boolean.toString(rs.getBoolean("ecinternationalorganizationeurinterests")), info));
			o
				.setEcinternationalorganization(
					field(Boolean.toString(rs.getBoolean("ecinternationalorganization")), info));
			o.setEcenterprise(field(Boolean.toString(rs.getBoolean("ecenterprise")), info));
			o.setEcsmevalidated(field(Boolean.toString(rs.getBoolean("ecsmevalidated")), info));
			o.setEcnutscode(field(Boolean.toString(rs.getBoolean("ecnutscode")), info));
			o.setCountry(prepareQualifierSplitting(rs.getString("country")));
			o.setDataInfo(info);
			o.setLastupdatetimestamp(lastUpdateTimestamp);

			return Arrays.asList(o);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processDatasourceOrganization(final ResultSet rs) {
		try {
			final DataInfo info = prepareDataInfo(rs);
			final String orgId = createOpenaireId(20, rs.getString("organization"), true);
			final String dsId = createOpenaireId(10, rs.getString("datasource"), true);
			final List<KeyValue> collectedFrom = listKeyValues(
				createOpenaireId(10, rs.getString("collectedfromid"), true), rs.getString("collectedfromname"));

			final Relation r1 = new Relation();
			r1.setRelType(DATASOURCE_ORGANIZATION);
			r1.setSubRelType(PROVISION);
			r1.setRelClass(IS_PROVIDED_BY);
			r1.setSource(dsId);
			r1.setTarget(orgId);
			r1.setCollectedfrom(collectedFrom);
			r1.setDataInfo(info);
			r1.setLastupdatetimestamp(lastUpdateTimestamp);

			final Relation r2 = new Relation();
			r2.setRelType(DATASOURCE_ORGANIZATION);
			r2.setSubRelType(PROVISION);
			r2.setRelClass(PROVIDES);
			r2.setSource(orgId);
			r2.setTarget(dsId);
			r2.setCollectedfrom(collectedFrom);
			r2.setDataInfo(info);
			r2.setLastupdatetimestamp(lastUpdateTimestamp);

			return Arrays.asList(r1, r2);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processProjectOrganization(final ResultSet rs) {
		try {
			final DataInfo info = prepareDataInfo(rs);
			final String orgId = createOpenaireId(20, rs.getString("resporganization"), true);
			final String projectId = createOpenaireId(40, rs.getString("project"), true);
			final List<KeyValue> collectedFrom = listKeyValues(
				createOpenaireId(10, rs.getString("collectedfromid"), true), rs.getString("collectedfromname"));

			final Relation r1 = new Relation();
			r1.setRelType(PROJECT_ORGANIZATION);
			r1.setSubRelType(PARTICIPATION);
			r1.setRelClass(HAS_PARTICIPANT);
			r1.setSource(projectId);
			r1.setTarget(orgId);
			r1.setCollectedfrom(collectedFrom);
			r1.setDataInfo(info);
			r1.setLastupdatetimestamp(lastUpdateTimestamp);

			final Relation r2 = new Relation();
			r2.setRelType(PROJECT_ORGANIZATION);
			r2.setSubRelType(PARTICIPATION);
			r2.setRelClass(IS_PARTICIPANT);
			r2.setSource(orgId);
			r2.setTarget(projectId);
			r2.setCollectedfrom(collectedFrom);
			r2.setDataInfo(info);
			r2.setLastupdatetimestamp(lastUpdateTimestamp);

			return Arrays.asList(r1, r2);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processClaims(final ResultSet rs) {

		final DataInfo info = dataInfo(
			false, null, false, false,
			qualifier(USER_CLAIM, USER_CLAIM, DNET_PROVENANCE_ACTIONS, DNET_PROVENANCE_ACTIONS), "0.9");

		final List<KeyValue> collectedFrom = listKeyValues(
			createOpenaireId(10, "infrastruct_::openaire", true), "OpenAIRE");

		try {
			if (rs.getString(SOURCE_TYPE).equals("context")) {
				final Result r;

				if (rs.getString(TARGET_TYPE).equals("dataset")) {
					r = new Dataset();
					r.setResulttype(DATASET_DEFAULT_RESULTTYPE);
				} else if (rs.getString(TARGET_TYPE).equals("software")) {
					r = new Software();
					r.setResulttype(SOFTWARE_DEFAULT_RESULTTYPE);
				} else if (rs.getString(TARGET_TYPE).equals("other")) {
					r = new OtherResearchProduct();
					r.setResulttype(ORP_DEFAULT_RESULTTYPE);
				} else {
					r = new Publication();
					r.setResulttype(PUBLICATION_DEFAULT_RESULTTYPE);
				}
				r.setId(createOpenaireId(50, rs.getString("target_id"), false));
				r.setLastupdatetimestamp(lastUpdateTimestamp);
				r.setContext(prepareContext(rs.getString("source_id"), info));
				r.setDataInfo(info);
				r.setCollectedfrom(collectedFrom);

				return Arrays.asList(r);
			} else {
				final String sourceId = createOpenaireId(rs.getString(SOURCE_TYPE), rs.getString("source_id"), false);
				final String targetId = createOpenaireId(rs.getString(TARGET_TYPE), rs.getString("target_id"), false);

				final Relation r1 = new Relation();
				final Relation r2 = new Relation();

				if (rs.getString(SOURCE_TYPE).equals("project")) {
					r1.setCollectedfrom(collectedFrom);
					r1.setRelType(RESULT_PROJECT);
					r1.setSubRelType(OUTCOME);
					r1.setRelClass(PRODUCES);

					r2.setCollectedfrom(collectedFrom);
					r2.setRelType(RESULT_PROJECT);
					r2.setSubRelType(OUTCOME);
					r2.setRelClass(IS_PRODUCED_BY);
				} else {
					r1.setCollectedfrom(collectedFrom);
					r1.setRelType(RESULT_RESULT);
					r1.setSubRelType(RELATIONSHIP);
					r1.setRelClass(IS_RELATED_TO);

					r2.setCollectedfrom(collectedFrom);
					r2.setRelType(RESULT_RESULT);
					r2.setSubRelType(RELATIONSHIP);
					r2.setRelClass(IS_RELATED_TO);
				}

				r1.setSource(sourceId);
				r1.setTarget(targetId);
				r1.setDataInfo(info);
				r1.setLastupdatetimestamp(lastUpdateTimestamp);

				r2.setSource(targetId);
				r2.setTarget(sourceId);
				r2.setDataInfo(info);
				r2.setLastupdatetimestamp(lastUpdateTimestamp);

				return Arrays.asList(r1, r2);
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
		return dataInfo(
				deletedbyinference,
				inferenceprovenance,
				inferred,
				false,
				ENTITYREGISTRY_PROVENANCE_ACTION,
				trust);
	}

	private Qualifier prepareQualifierSplitting(final String s) {
		if (StringUtils.isBlank(s)) {
			return null;
		}
		final String[] arr = s.split("@@@");
		return arr.length == 4 ? qualifier(arr[0], arr[1], arr[2], arr[3]) : null;
	}

	private List<Field<String>> prepareListFields(final Array array, final DataInfo info) {
		try {
			return array != null ? listFields(info, (String[]) array.getArray()) : new ArrayList<>();
		} catch (final SQLException e) {
			throw new RuntimeException("Invalid SQL array", e);
		}
	}

	private StructuredProperty prepareStructProp(final String s, final DataInfo dataInfo) {
		if (StringUtils.isBlank(s)) {
			return null;
		}
		final String[] parts = s.split("###");
		if (parts.length == 2) {
			final String value = parts[0];
			final String[] arr = parts[1].split("@@@");
			if (arr.length == 4) {
				return structuredProperty(value, arr[0], arr[1], arr[2], arr[3], dataInfo);
			}
		}
		return null;
	}

	private List<StructuredProperty> prepareListOfStructProps(
		final Array array,
		final DataInfo dataInfo) throws SQLException {
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
				final String issn = StringUtils.isNotBlank(arr[0]) ? arr[0].trim() : null;
				final String eissn = StringUtils.isNotBlank(arr[1]) ? arr[1].trim() : null;

				final String lissn = StringUtils.isNotBlank(arr[2]) ? arr[2].trim() : null;

				if (issn != null || eissn != null || lissn != null) {
					return journal(name, issn, eissn, eissn, null, null, null, null, null, null, null, info);
				}
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
