
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.schema.common.ModelConstants.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.*;
import static eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory.*;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.DbClient;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.oa.graph.raw.common.AbstractMigrationApplication;
import eu.dnetlib.dhp.oa.graph.raw.common.MigrateAction;
import eu.dnetlib.dhp.oa.graph.raw.common.VerifyNsPrefixPredicate;

import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;

public class MigrateDbEntitiesApplication extends AbstractMigrationApplication implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(MigrateDbEntitiesApplication.class);

	private static final EntityDataInfo ENTITY_DATA_INFO_CLAIM = dataInfo(
		false, false, 0.9f, null, false, qualifier(USER_CLAIM, USER_CLAIM, DNET_PROVENANCE_ACTIONS));

	private static final DataInfo REL_DATA_INFO_CLAIM = dataInfo(
			0.9f, null, false, qualifier(USER_CLAIM, USER_CLAIM, DNET_PROVENANCE_ACTIONS));

	private static final List<KeyValue> COLLECTED_FROM_CLAIM = listKeyValues(
			createOpenaireId(10, "infrastruct_::openaire", true), "OpenAIRE");

	private final static List<Provenance> PROVENANCE_CLAIM = getProvenance(COLLECTED_FROM_CLAIM, ENTITY_DATA_INFO_CLAIM);

	public static final String SOURCE_TYPE = "source_type";
	public static final String TARGET_TYPE = "target_type";

	private final DbClient dbClient;

	private final long lastUpdateTimestamp;

	private final VocabularyGroup vocs;

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					MigrateDbEntitiesApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/migrate_db_entities_parameters.json")));

		parser.parseArgument(args);

		final String dbUrl = parser.get("postgresUrl");
		log.info("postgresUrl: {}", dbUrl);

		final String dbUser = parser.get("postgresUser");
		log.info("postgresUser: {}", dbUser);

		final String dbPassword = parser.get("postgresPassword");
		log.info("postgresPassword: xxx");

		final String dbSchema = parser.get("dbschema");
		log.info("dbSchema {}: " + dbSchema);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final String hdfsPath = parser.get("hdfsPath");
		log.info("hdfsPath: {}", hdfsPath);

		final String nsPrefixBlacklist = parser.get("nsPrefixBlacklist");
		log.info("nsPrefixBlacklist: {}", nsPrefixBlacklist);

		final Predicate<Oaf> verifyNamespacePrefix = new VerifyNsPrefixPredicate(nsPrefixBlacklist);

		final MigrateAction process = parser.get("action") != null ? MigrateAction.valueOf(parser.get("action"))
			: MigrateAction.openaire;
		log.info("migrateAction: {}", process);

		try (final MigrateDbEntitiesApplication smdbe = new MigrateDbEntitiesApplication(hdfsPath, dbUrl, dbUser,
			dbPassword, isLookupUrl)) {

			switch (process) {
				case claims:
					log.info("Processing claims...");
					smdbe.execute("queryClaims.sql", smdbe::processClaims);
					break;
				case openaire:
					log.info("Processing services...");
					smdbe.execute("queryServices.sql", smdbe::processService, verifyNamespacePrefix);

					log.info("Processing projects...");
					if (dbSchema.equalsIgnoreCase("beta")) {
						smdbe.execute("queryProjects.sql", smdbe::processProject, verifyNamespacePrefix);
					} else {
						smdbe.execute("queryProjects_production.sql", smdbe::processProject, verifyNamespacePrefix);
					}

					log.info("Processing Organizations...");
					smdbe.execute("queryOrganizations.sql", smdbe::processOrganization, verifyNamespacePrefix);

					log.info("Processing relations services <-> orgs ...");
					smdbe
						.execute(
							"queryServiceOrganization.sql", smdbe::processServiceOrganization,
							verifyNamespacePrefix);

					log.info("Processing projects <-> orgs ...");
					smdbe
						.execute(
							"queryProjectOrganization.sql", smdbe::processProjectOrganization, verifyNamespacePrefix);
					break;
				case openorgs_dedup: // generates organization entities and relations for openorgs dedup
					log.info("Processing Openorgs...");
					smdbe
						.execute("queryOpenOrgsForOrgsDedup.sql", smdbe::processOrganization, verifyNamespacePrefix);

					log.info("Processing Openorgs Sim Rels...");
					smdbe.execute("queryOpenOrgsSimilarityForOrgsDedup.sql", smdbe::processOrgOrgSimRels);
					break;

				case openorgs: // generates organization entities and relations for provision
					log.info("Processing Openorgs For Provision...");
					smdbe
						.execute("queryOpenOrgsForProvision.sql", smdbe::processOrganization, verifyNamespacePrefix);

					log.info("Processing Openorgs Merge Rels...");
					smdbe.execute("queryOpenOrgsSimilarityForProvision.sql", smdbe::processOrgOrgMergeRels);

					log.info("Processing Openorgs Parent/Child Rels...");
					smdbe.execute("queryParentChildRelsOpenOrgs.sql", smdbe::processOrgOrgParentChildRels);
					break;

				case openaire_organizations:

					log.info("Processing Organizations...");
					smdbe.execute("queryOrganizations.sql", smdbe::processOrganization, verifyNamespacePrefix);
					break;
			}
			log.info("All done.");
		}
	}

	protected MigrateDbEntitiesApplication(final VocabularyGroup vocs) { // ONLY FOR UNIT TEST
		super();
		this.dbClient = null;
		this.lastUpdateTimestamp = new Date().getTime();
		this.vocs = vocs;
	}

	public MigrateDbEntitiesApplication(
		final String hdfsPath, final String dbUrl, final String dbUser, final String dbPassword,
		final String isLookupUrl)
		throws Exception {
		super(hdfsPath);
		this.dbClient = new DbClient(dbUrl, dbUser, dbPassword);
		this.lastUpdateTimestamp = new Date().getTime();
		this.vocs = VocabularyGroup.loadVocsFromIS(ISLookupClientFactory.getLookUpService(isLookupUrl));
	}

	public void execute(final String sqlFile, final Function<ResultSet, List<Oaf>> producer)
		throws Exception {
		execute(sqlFile, producer, oaf -> true);
	}

	public void execute(final String sqlFile,
		final Function<ResultSet, List<Oaf>> producer,
		final Predicate<Oaf> predicate)
		throws Exception {
		final String sql = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/sql/" + sqlFile));

		final Consumer<ResultSet> consumer = rs -> producer.apply(rs).forEach(oaf -> {
			if (predicate.test(oaf)) {
				emitOaf(oaf);
			}
		});

		dbClient.processResults(sql, consumer);
	}

	public List<Oaf> processService(final ResultSet rs) {
		try {
			final EntityDataInfo info = prepareDataInfo(rs);

			final Datasource ds = new Datasource();

			ds.setId(createOpenaireId(10, rs.getString("id"), true));
			ds
				.setOriginalId(
					Arrays
						.asList((String[]) rs.getArray("originalid").getArray())
						.stream()
						.filter(StringUtils::isNotBlank)
						.collect(Collectors.toList()));
			ds.setCollectedfrom(prepareCollectedfrom(rs.getArray("collectedfrom")));
			ds.setPid(prepareListOfStructProps(rs.getArray("pid")));
			ds.setDateofcollection(asString(rs.getDate("dateofcollection")));
			ds.setDateoftransformation(null); // Value not returned by the SQL query
			ds.setExtraInfo(new ArrayList<>()); // Values not present in the DB
			ds.setDatasourcetype(prepareQualifierSplitting(rs.getString("datasourcetype")));
			ds.setDatasourcetypeui(prepareQualifierSplitting(rs.getString("datasourcetypeui")));
			ds.setEosctype(prepareQualifierSplitting(rs.getString("eosctype")));
			ds.setEoscdatasourcetype(prepareQualifierSplitting(rs.getString("eoscdatasourcetype")));
			ds.setOpenairecompatibility(prepareQualifierSplitting(rs.getString("openairecompatibility")));
			ds.setOfficialname(rs.getString("officialname"));
			ds.setEnglishname(rs.getString("englishname"));
			ds.setWebsiteurl(rs.getString("websiteurl"));
			ds.setLogourl(rs.getString("logourl"));
			ds.setContactemail(rs.getString("contactemail"));
			ds.setNamespaceprefix(rs.getString("namespaceprefix"));
			ds.setLatitude(Double.toString(rs.getDouble("latitude")));
			ds.setLongitude(Double.toString(rs.getDouble("longitude")));
			ds.setDateofvalidation(asString(rs.getDate("dateofvalidation")));
			ds.setDescription(rs.getString("description"));
			ds.setSubjects(prepareListOfStructProps(rs.getArray("subjects")));
			ds.setOdnumberofitems(Double.toString(rs.getInt("odnumberofitems")));
			ds.setOdnumberofitemsdate(asString(rs.getDate("odnumberofitemsdate")));
			ds.setOdpolicies(rs.getString("odpolicies"));
			ds.setOdlanguages(prepareListFields(rs.getArray("odlanguages")));
			ds.setLanguages(listValues(rs.getArray("languages")));
			ds.setAccessinfopackage(prepareListFields(rs.getArray("accessinfopackage")));
			ds.setReleasestartdate(asString(rs.getDate("releasestartdate")));
			ds.setReleaseenddate(asString(rs.getDate("releaseenddate")));
			ds.setMissionstatementurl(rs.getString("missionstatementurl"));
			ds.setDatabaseaccesstype(rs.getString("databaseaccesstype"));
			ds.setDatauploadtype(rs.getString("datauploadtype"));
			ds.setDatabaseaccessrestriction(rs.getString("databaseaccessrestriction"));
			ds.setDatauploadrestriction(rs.getString("datauploadrestriction"));
			ds.setVersioning(rs.getBoolean("versioning"));
			ds.setVersioncontrol(rs.getBoolean("versioncontrol"));
			ds.setCitationguidelineurl(rs.getString("citationguidelineurl"));

			ds.setPidsystems(rs.getString("pidsystems"));
			ds.setCertificates(rs.getString("certificates"));
			ds.setPolicies(new ArrayList<>()); // The sql query returns an empty array
			ds
				.setJournal(
					journal(
						rs.getString("officialname"), rs.getString("issnPrinted"), rs.getString("issnOnline"),
						rs.getString("issnLinking"))); // Journal

			ds.setResearchentitytypes(listValues(rs.getArray("researchentitytypes")));
			ds.setJurisdiction(prepareQualifierSplitting(rs.getString("jurisdiction")));
			ds.setThematic(rs.getBoolean("thematic"));
			ds.setContentpolicies(prepareListOfQualifiers(rs.getArray("contentpolicies")));
			ds.setSubmissionpolicyurl(rs.getString("submissionpolicyurl"));
			ds.setPreservationpolicyurl(rs.getString("preservationpolicyurl"));
			ds.setResearchproductaccesspolicies(listValues(rs.getArray("researchproductaccesspolicies")));
			ds
				.setResearchproductmetadataaccesspolicies(
					listValues(rs.getArray("researchproductmetadataaccesspolicies")));

			ds.setConsenttermsofuse(rs.getBoolean("consenttermsofuse"));
			ds.setFulltextdownload(rs.getBoolean("fulltextdownload"));
			ds
				.setConsenttermsofusedate(
					Optional
						.ofNullable(
							rs.getDate("consenttermsofusedate"))
						.map(java.sql.Date::toString)
						.orElse(null));
			ds
				.setLastconsenttermsofusedate(
					Optional
						.ofNullable(
							rs.getDate("lastconsenttermsofusedate"))
						.map(java.sql.Date::toString)
						.orElse(null));

			ds.setDataInfo(info);
			ds.setLastupdatetimestamp(lastUpdateTimestamp);

			return Arrays.asList(ds);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processProject(final ResultSet rs) {
		try {
			final EntityDataInfo info = prepareDataInfo(rs);

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
			p.setWebsiteurl(rs.getString("websiteurl"));
			p.setCode(rs.getString("code"));
			p.setAcronym(rs.getString("acronym"));
			p.setTitle(rs.getString("title"));
			p.setStartdate(asString(rs.getDate("startdate")));
			p.setEnddate(asString(rs.getDate("enddate")));
			p.setCallidentifier(rs.getString("callidentifier"));
			p.setKeywords(rs.getString("keywords"));
			p.setDuration(Integer.toString(rs.getInt("duration")));
			p.setEcsc39(Boolean.toString(rs.getBoolean("ecsc39")));
			p
				.setOamandatepublications(Boolean.toString(rs.getBoolean("oamandatepublications")));
			p.setEcarticle29_3(Boolean.toString(rs.getBoolean("ecarticle29_3")));
			p.setSubjects(prepareListOfStructProps(rs.getArray("subjects")));
			p.setFundingtree(prepareListFields(rs.getArray("fundingtree")));
			p.setContracttype(prepareQualifierSplitting(rs.getString("contracttype")));
			p.setOptional1(rs.getString("optional1"));
			p.setOptional2(rs.getString("optional2"));
			p.setJsonextrainfo(rs.getString("jsonextrainfo"));
			p.setContactfullname(rs.getString("contactfullname"));
			p.setContactfax(rs.getString("contactfax"));
			p.setContactphone(rs.getString("contactphone"));
			p.setContactemail(rs.getString("contactemail"));
			p.setSummary(rs.getString("summary"));
			p.setCurrency(rs.getString("currency"));
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
			final EntityDataInfo info = prepareDataInfo(rs);

			final Organization o = new Organization();

			o.setId(createOpenaireId(20, rs.getString("organizationid"), true));
			o.setOriginalId(Arrays.asList(rs.getString("organizationid")));
			o
				.setCollectedfrom(
					listKeyValues(
						createOpenaireId(10, rs.getString("collectedfromid"), true),
						rs.getString("collectedfromname")));
			o.setPid(prepareListOfStructProps(rs.getArray("pid")));
			o.setDateofcollection(asString(rs.getDate("dateofcollection")));
			o.setDateoftransformation(asString(rs.getDate("dateoftransformation")));
			o.setExtraInfo(new ArrayList<>()); // Values not present in the DB
			o.setLegalshortname(rs.getString("legalshortname"));
			o.setLegalname(rs.getString("legalname"));
			o.setAlternativeNames(prepareListFields(rs.getArray("alternativenames")));
			o.setWebsiteurl(rs.getString("websiteurl"));
			o.setLogourl(rs.getString("logourl"));
			o.setEclegalbody(Boolean.toString(rs.getBoolean("eclegalbody")));
			o.setEclegalperson(Boolean.toString(rs.getBoolean("eclegalperson")));
			o.setEcnonprofit(Boolean.toString(rs.getBoolean("ecnonprofit")));
			o
				.setEcresearchorganization(Boolean.toString(rs.getBoolean("ecresearchorganization")));
			o.setEchighereducation(Boolean.toString(rs.getBoolean("echighereducation")));
			o
				.setEcinternationalorganizationeurinterests(
					Boolean.toString(rs.getBoolean("ecinternationalorganizationeurinterests")));
			o
				.setEcinternationalorganization(
					Boolean.toString(rs.getBoolean("ecinternationalorganization")));
			o.setEcenterprise(Boolean.toString(rs.getBoolean("ecenterprise")));
			o.setEcsmevalidated(Boolean.toString(rs.getBoolean("ecsmevalidated")));
			o.setEcnutscode(Boolean.toString(rs.getBoolean("ecnutscode")));
			o.setCountry(prepareQualifierSplitting(rs.getString("country")));
			o.setDataInfo(info);
			o.setLastupdatetimestamp(lastUpdateTimestamp);

			return Arrays.asList(o);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processServiceOrganization(final ResultSet rs) {
		try {
			final DataInfo info = prepareRelDataInfo(rs);
			final String orgId = createOpenaireId(20, rs.getString("organization"), true);
			final String dsId = createOpenaireId(10, rs.getString("service"), true);
			final List<KeyValue> collectedFrom = listKeyValues(
				createOpenaireId(10, rs.getString("collectedfromid"), true), rs.getString("collectedfromname"));

			final List<Provenance> provenance = getProvenance(collectedFrom, info);
			return Arrays.asList(OafMapperUtils
					.getRelation(
							orgId, dsId, DATASOURCE_ORGANIZATION, PROVISION, PROVIDES, provenance));
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processProjectOrganization(final ResultSet rs) {
		try {
			final DataInfo info = prepareRelDataInfo(rs);
			final String orgId = createOpenaireId(20, rs.getString("resporganization"), true);
			final String projectId = createOpenaireId(40, rs.getString("project"), true);
			final List<KeyValue> collectedFrom = listKeyValues(
				createOpenaireId(10, rs.getString("collectedfromid"), true), rs.getString("collectedfromname"));

			final List<Provenance> provenance = getProvenance(collectedFrom, info);

			final List<KeyValue> properties = Lists
				.newArrayList(
					keyValue("contribution", String.valueOf(rs.getDouble("contribution"))),
					keyValue("currency", rs.getString("currency")));

			return Arrays.asList(
					OafMapperUtils.getRelation(
					orgId, projectId, PROJECT_ORGANIZATION, PARTICIPATION, IS_PARTICIPANT, provenance, properties));

		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processClaims(final ResultSet rs) {
		try {
			final String sourceType = rs.getString(SOURCE_TYPE);
			final String targetType = rs.getString(TARGET_TYPE);
			if (sourceType.equals("context")) {
				final Result r;

				if (targetType.equals("dataset")) {
					r = new Dataset();
					r.setResulttype(DATASET_DEFAULT_RESULTTYPE.getClassid());
				} else if (targetType.equals("software")) {
					r = new Software();
					r.setResulttype(SOFTWARE_DEFAULT_RESULTTYPE.getClassid());
				} else if (targetType.equals("other")) {
					r = new OtherResearchProduct();
					r.setResulttype(ORP_DEFAULT_RESULTTYPE.getClassid());
				} else {
					r = new Publication();
					r.setResulttype(PUBLICATION_DEFAULT_RESULTTYPE.getClassid());
				}
				r.setId(createOpenaireId(50, rs.getString("target_id"), false));
				r.setLastupdatetimestamp(lastUpdateTimestamp);
				r.setContext(prepareContext(rs.getString("source_id"), ENTITY_DATA_INFO_CLAIM));
				r.setDataInfo(ENTITY_DATA_INFO_CLAIM);
				r.setCollectedfrom(COLLECTED_FROM_CLAIM);

				return Arrays.asList(r);
			} else {
				final String validationDate = rs.getString("curation_date");

				final String sourceId = createOpenaireId(sourceType, rs.getString("source_id"), false);
				final String targetId = createOpenaireId(targetType, rs.getString("target_id"), false);

				Relation rel = prepareRelation(sourceId, targetId, PROVENANCE_CLAIM, validationDate);

				final String semantics = rs.getString("semantics");

				switch (semantics) {
					case "resultResult_relationship_isRelatedTo":
						rel = setRelationSemantic(rel, RESULT_RESULT, RELATIONSHIP, IS_RELATED_TO);
						break;
					case "resultProject_outcome_produces":
						if (!"project".equals(sourceType)) {
							throw new IllegalStateException(
								String
									.format(
										"invalid claim, sourceId: %s, targetId: %s, semantics: %s", sourceId, targetId,
										semantics));
						}
						rel = setRelationSemantic(rel, RESULT_PROJECT, OUTCOME, PRODUCES);
						break;
					case "resultResult_publicationDataset_isRelatedTo":
						rel = setRelationSemantic(rel, RESULT_RESULT, PUBLICATION_DATASET, IS_RELATED_TO);
						break;
					default:
						throw new IllegalArgumentException("claim semantics not managed: " + semantics);
				}

				return Arrays.asList(rel);
			}
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Relation prepareRelation(final String sourceId, final String targetId, final List<Provenance> provenance, final String validationDate) {
		final Relation r = new Relation();
		if (StringUtils.isNotBlank(validationDate)) {
			r.setValidated(true);
			r.setValidationDate(validationDate);
		}
		r.setProvenance(provenance);
		r.setSource(sourceId);
		r.setTarget(targetId);
		return r;
	}

	private Relation setRelationSemantic(final Relation r, final String relType, final String subRelType,
		final String relClass) {
		r.setRelType(relType);
		r.setSubRelType(subRelType);
		r.setRelClass(relClass);
		return r;
	}

	private List<Context> prepareContext(final String id, final DataInfo dataInfo) {
		final Context context = new Context();
		context.setId(id);
		context.setDataInfo(Arrays.asList(dataInfo));
		return Arrays.asList(context);
	}

	private EntityDataInfo prepareDataInfo(final ResultSet rs) throws SQLException {
		final Boolean deletedbyinference = rs.getBoolean("deletedbyinference");
		final String inferenceprovenance = rs.getString("inferenceprovenance");
		final Boolean inferred = rs.getBoolean("inferred");

		final float trust = (float) rs.getDouble("trust");

		return dataInfo(false, deletedbyinference, trust, inferenceprovenance, inferred, ENTITYREGISTRY_PROVENANCE_ACTION);
	}

	private DataInfo prepareRelDataInfo(final ResultSet rs) throws SQLException {
		final String inferenceprovenance = rs.getString("inferenceprovenance");
		final Boolean inferred = rs.getBoolean("inferred");
		final float trust = (float) rs.getDouble("trust");

		return dataInfo(trust, inferenceprovenance, inferred, ENTITYREGISTRY_PROVENANCE_ACTION);
	}

	private List<KeyValue> prepareCollectedfrom(Array values) throws SQLException {
		if (Objects.isNull(values)) {
			return null;
		}
		return Arrays
			.stream((String[]) values.getArray())
			.filter(Objects::nonNull)
			.distinct()
			.map(s -> keyValueSplitting(s, "@@@"))
			.collect(Collectors.toList());
	}

	public static KeyValue keyValueSplitting(final String s, String separator) {
		if (StringUtils.isBlank(s)) {
			return null;
		}
		final String[] arr = s.split(separator);
		if (arr.length != 2) {
			return null;
		}
		KeyValue kv = new KeyValue();
		kv.setKey(createOpenaireId(10, arr[0], true));
		kv.setValue(arr[1]);
		return kv;
	}

	private Qualifier prepareQualifierSplitting(final String s) {
		if (StringUtils.isBlank(s)) {
			return null;
		}
		final String[] arr = s.split("@@@");
		return arr.length == 2 ? vocs.getTermAsQualifier(arr[1], arr[0]) : null;
	}

	private List<String> prepareListFields(final Array array) {
		try {
			return array != null ? listValues(array) : new ArrayList<>();
		} catch (final SQLException e) {
			throw new RuntimeException("Invalid SQL array", e);
		}
	}

	private StructuredProperty prepareStructProp(final String s) {
		if (StringUtils.isBlank(s)) {
			return null;
		}
		final String[] parts = s.split("###");
		if (parts.length == 2) {
			final String value = parts[0];
			final String[] arr = parts[1].split("@@@");
			if (arr.length == 2) {
				return structuredProperty(value, vocs.getTermAsQualifier(arr[1], arr[0]));
			}
		}
		return null;
	}

	private List<StructuredProperty> prepareListOfStructProps(
		final Array array) throws SQLException {
		final List<StructuredProperty> res = new ArrayList<>();
		if (array != null) {
			for (final String s : (String[]) array.getArray()) {
				final StructuredProperty sp = prepareStructProp(s);
				if (sp != null) {
					res.add(sp);
				}
			}
		}

		return res;
	}

	private List<Qualifier> prepareListOfQualifiers(final Array array) throws SQLException {
		final List<Qualifier> res = new ArrayList<>();
		if (array != null) {
			for (final String s : (String[]) array.getArray()) {
				final Qualifier q = prepareQualifierSplitting(s);
				if (q != null) {
					res.add(q);
				}
			}
		}
		return res;
	}

	public List<Oaf> processOrgOrgMergeRels(final ResultSet rs) {
		try {
			final DataInfo info = prepareDataInfo(rs); // TODO

			final String orgId1 = createOpenaireId(20, rs.getString("id1"), true);
			final String orgId2 = createOpenaireId(20, rs.getString("id2"), true);

			final List<KeyValue> collectedFrom = listKeyValues(
				createOpenaireId(10, rs.getString("collectedfromid"), true), rs.getString("collectedfromname"));

			final List<Provenance> provenance = getProvenance(collectedFrom, info);
			return Arrays.asList(getRelation(orgId1, orgId2, ORG_ORG_RELTYPE, DEDUP, MERGES, provenance));
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processOrgOrgParentChildRels(final ResultSet rs) {
		try {
			final DataInfo info = prepareDataInfo(rs); // TODO

			final String orgId1 = createOpenaireId(20, rs.getString("source"), true);
			final String orgId2 = createOpenaireId(20, rs.getString("target"), true);

			final List<KeyValue> collectedFrom = listKeyValues(
				createOpenaireId(10, rs.getString("collectedfromid"), true), rs.getString("collectedfromname"));

			final List<Provenance> provenance = getProvenance(collectedFrom, info);

			final String relClass = rs.getString("type");
			return Arrays.asList(getRelation(orgId1, orgId2, ORG_ORG_RELTYPE, RELATIONSHIP, relClass, provenance));
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<Oaf> processOrgOrgSimRels(final ResultSet rs) {
		try {
			final DataInfo info = prepareDataInfo(rs); // TODO

			final String orgId1 = createOpenaireId(20, rs.getString("id1"), true);
			final String orgId2 = createOpenaireId(20, rs.getString("id2"), true);
			final String relClass = rs.getString("relclass");

			final List<KeyValue> collectedFrom = listKeyValues(
				createOpenaireId(10, rs.getString("collectedfromid"), true), rs.getString("collectedfromname"));

			final List<Provenance> provenance = getProvenance(collectedFrom, info);

			return Arrays.asList(getRelation(orgId1, orgId2, ORG_ORG_RELTYPE, DEDUP, relClass, provenance));
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		dbClient.close();
	}

}
