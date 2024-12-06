
package eu.dnetlib.dhp.oa.graph.raw;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.common.RelationInverse;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

@ExtendWith(MockitoExtension.class)
class MigrateDbEntitiesApplicationTest {

	private MigrateDbEntitiesApplication app;

	@Mock(lenient = true)
	private ResultSet rs;

	@Mock
	private VocabularyGroup vocs;

	@BeforeEach
	public void setUp() {
		lenient()
			.when(vocs.getTermAsQualifier(anyString(), anyString()))
			.thenAnswer(
				invocation -> OafMapperUtils
					.qualifier(
						invocation.getArgument(1), invocation.getArgument(1), invocation.getArgument(0),
						invocation.getArgument(0)));

		lenient().when(vocs.termExists(anyString(), anyString())).thenReturn(true);

		this.app = new MigrateDbEntitiesApplication(vocs);
	}

	@Test
	void testProcessService() throws Exception {
		final List<TypedField> fields = prepareMocks("services_resultset_entry.json");

		final List<Oaf> list = app.processService(rs);
		assertEquals(1, list.size());
		verifyMocks(fields);

		final Datasource ds = (Datasource) list.get(0);
		assertValidId(ds.getId());
		ds
			.getCollectedfrom()
			.stream()
			.map(KeyValue::getKey)
			.forEach(this::assertValidId);

		assertEquals(1, ds.getPid().size());
		assertEquals("r3d100010218", ds.getPid().get(0).getValue());
		assertEquals("re3data", ds.getPid().get(0).getQualifier().getClassid());
		assertEquals("dnet:pid_types", ds.getPid().get(0).getQualifier().getSchemeid());

		assertEquals(getValueAsString("officialname", fields), ds.getOfficialname().getValue());
		assertEquals(getValueAsString("englishname", fields), ds.getEnglishname().getValue());
		assertEquals(getValueAsString("websiteurl", fields), ds.getWebsiteurl().getValue());
		assertEquals(getValueAsString("logourl", fields), ds.getLogourl());
		assertEquals(getValueAsString("contactemail", fields), ds.getContactemail().getValue());
		assertEquals(getValueAsString("namespaceprefix", fields), ds.getNamespaceprefix().getValue());
		assertEquals(getValueAsString("officialname", fields), ds.getJournal().getName());
		assertEquals(getValueAsString("issnPrinted", fields), ds.getJournal().getIssnPrinted());
		assertEquals(getValueAsString("issnOnline", fields), ds.getJournal().getIssnOnline());
		assertEquals(getValueAsString("issnLinking", fields), ds.getJournal().getIssnLinking());

		assertEquals("pubsrepository::journal", ds.getDatasourcetype().getClassid());
		assertEquals("dnet:datasource_typologies", ds.getDatasourcetype().getSchemeid());

		assertEquals("pubsrepository::journal", ds.getDatasourcetypeui().getClassid());
		assertEquals("dnet:datasource_typologies_ui", ds.getDatasourcetypeui().getSchemeid());

		assertEquals("Data Source", ds.getEosctype().getClassid());
		assertEquals("Data Source", ds.getEosctype().getClassname());
		assertEquals("dnet:eosc_types", ds.getEosctype().getSchemeid());
		assertEquals("dnet:eosc_types", ds.getEosctype().getSchemename());

		assertEquals("Journal archive", ds.getEoscdatasourcetype().getClassid());
		assertEquals("Journal archive", ds.getEoscdatasourcetype().getClassname());
		assertEquals("dnet:eosc_datasource_types", ds.getEoscdatasourcetype().getSchemeid());
		assertEquals("dnet:eosc_datasource_types", ds.getEoscdatasourcetype().getSchemename());

		assertEquals("openaire4.0", ds.getOpenairecompatibility().getClassid());
		assertEquals("openaire4.0", ds.getOpenairecompatibility().getClassname());
		assertEquals("dnet:datasourceCompatibilityLevel", ds.getOpenairecompatibility().getSchemeid());
		assertEquals("dnet:datasourceCompatibilityLevel", ds.getOpenairecompatibility().getSchemename());

		assertEquals(getValueAsDouble("latitude", fields).toString(), ds.getLatitude().getValue());
		assertEquals(getValueAsDouble("longitude", fields).toString(), ds.getLongitude().getValue());
		assertEquals(getValueAsString("dateofvalidation", fields), ds.getDateofvalidation());

		assertEquals(getValueAsString("description", fields), ds.getDescription().getValue());

		// TODO assertEquals(getValueAsString("subjects", fields), ds.getSubjects());

		assertEquals("0.0", ds.getOdnumberofitems().getValue());
		assertEquals(getValueAsString("odnumberofitemsdate", fields), ds.getOdnumberofitemsdate());
		assertEquals(getValueAsString("odpolicies", fields), ds.getOdpolicies());

		assertEquals(
			getValueAsList("odlanguages", fields),
			ds.getOdlanguages().stream().map(Field::getValue).collect(Collectors.toList()));
		assertEquals(getValueAsList("languages", fields), ds.getLanguages());
		assertEquals(
			getValueAsList("accessinfopackage", fields),
			ds.getAccessinfopackage().stream().map(Field::getValue).collect(Collectors.toList()));
		assertEquals(getValueAsString("releasestartdate", fields), ds.getReleasestartdate());
		assertEquals(getValueAsString("releaseenddate", fields), ds.getReleasestartdate());
		assertEquals(getValueAsString("missionstatementurl", fields), ds.getMissionstatementurl());

		assertEquals(null, ds.getDataprovider());
		assertEquals(null, ds.getServiceprovider());

		assertEquals(getValueAsString("databaseaccesstype", fields), ds.getDatabaseaccesstype());
		assertEquals(getValueAsString("datauploadtype", fields), ds.getDatauploadtype());
		assertEquals(getValueAsString("databaseaccessrestriction", fields), ds.getDatabaseaccessrestriction());
		assertEquals(getValueAsString("datauploadrestriction", fields), ds.getDatauploadrestriction());

		assertEquals(false, ds.getVersioning().getValue());
		assertEquals(false, ds.getVersioncontrol());

		assertEquals(getValueAsString("citationguidelineurl", fields), ds.getCitationguidelineurl());
		assertEquals(getValueAsString("pidsystems", fields), ds.getPidsystems());
		assertEquals(getValueAsString("certificates", fields), ds.getCertificates());

		assertEquals(getValueAsList("researchentitytypes", fields), ds.getResearchentitytypes());

		assertEquals("National", ds.getJurisdiction().getClassid());
		assertEquals("eosc:jurisdictions", ds.getJurisdiction().getSchemeid());

		assertTrue(ds.getThematic());

		HashSet<String> cpSchemeId = ds
			.getContentpolicies()
			.stream()
			.map(Qualifier::getSchemeid)
			.collect(Collectors.toCollection(HashSet::new));
		assertEquals(1, cpSchemeId.size());
		assertTrue(cpSchemeId.contains("eosc:contentpolicies"));
		HashSet<String> cpSchemeName = ds
			.getContentpolicies()
			.stream()
			.map(Qualifier::getSchemename)
			.collect(Collectors.toCollection(HashSet::new));
		assertEquals(1, cpSchemeName.size());
		assertTrue(cpSchemeName.contains("eosc:contentpolicies"));
		assertEquals(2, ds.getContentpolicies().size());
		assertEquals("Taxonomic classification", ds.getContentpolicies().get(0).getClassid());
		assertEquals("Resource collection", ds.getContentpolicies().get(1).getClassid());

		assertEquals(getValueAsString("submissionpolicyurl", fields), ds.getSubmissionpolicyurl());
		assertEquals(getValueAsString("preservationpolicyurl", fields), ds.getPreservationpolicyurl());

		assertEquals(
			getValueAsList("researchproductaccesspolicies", fields),
			ds.getResearchproductaccesspolicies());
		assertEquals(
			getValueAsList("researchproductmetadataaccesspolicies", fields),
			ds.getResearchproductmetadataaccesspolicies());

		assertEquals(true, ds.getConsenttermsofuse());
		assertEquals(true, ds.getFulltextdownload());
		assertEquals("2022-03-11", ds.getConsenttermsofusedate());
		assertEquals("2022-03-11", ds.getLastconsenttermsofusedate());
	}

	@Test
	void testProcessProject() throws Exception {
		final List<TypedField> fields = prepareMocks("projects_resultset_entry.json");

		final List<Oaf> list = app.processProject(rs);
		assertEquals(1, list.size());
		verifyMocks(fields);

		final Project p = (Project) list.get(0);
		assertValidId(p.getId());
		assertValidId(p.getCollectedfrom().get(0).getKey());
		assertEquals(getValueAsString("acronym", fields), p.getAcronym().getValue());
		assertEquals(getValueAsString("title", fields), p.getTitle().getValue());
		assertEquals(getValueAsString("collectedfromname", fields), p.getCollectedfrom().get(0).getValue());
		assertEquals(getValueAsFloat("fundedamount", fields), p.getFundedamount());
		assertEquals(getValueAsFloat("totalcost", fields), p.getTotalcost());
	}

	@Test
	void testProcessOrganization() throws Exception {
		final List<TypedField> fields = prepareMocks("organizations_resultset_entry.json");

		final List<Oaf> list = app.processOrganization(rs);

		assertEquals(1, list.size());

		verifyMocks(fields);

		final Organization o = (Organization) list.get(0);
		assertValidId(o.getId());
		assertValidId(o.getCollectedfrom().get(0).getKey());
		assertEquals(getValueAsString("legalshortname", fields), o.getLegalshortname().getValue());
		assertEquals(getValueAsString("legalname", fields), o.getLegalname().getValue());
		assertEquals(getValueAsString("websiteurl", fields), o.getWebsiteurl().getValue());
		assertEquals(getValueAsString("country", fields).split("@@@")[0], o.getCountry().getClassid());
		assertEquals(getValueAsString("country", fields).split("@@@")[0], o.getCountry().getClassname());
		assertEquals(getValueAsString("country", fields).split("@@@")[1], o.getCountry().getSchemeid());
		assertEquals(getValueAsString("country", fields).split("@@@")[1], o.getCountry().getSchemename());
		assertEquals(getValueAsString("collectedfromname", fields), o.getCollectedfrom().get(0).getValue());
		final List<String> alternativenames = getValueAsList("alternativenames", fields);
		assertEquals(2, alternativenames.size());
		assertTrue(alternativenames.contains("Pippo"));
		assertTrue(alternativenames.contains("Foo"));
	}

	@Test
	void testProcessDatasourceOrganization() throws Exception {
		final List<TypedField> fields = prepareMocks("datasourceorganization_resultset_entry.json");

		final List<Oaf> list = app.processServiceOrganization(rs);

		assertEquals(2, list.size());
		verifyMocks(fields);

		final Relation r1 = (Relation) list.get(0);
		final Relation r2 = (Relation) list.get(1);
		assertValidId(r1.getSource());
		assertValidId(r2.getSource());
		assertEquals(r1.getSource(), r2.getTarget());
		assertEquals(r2.getSource(), r1.getTarget());

		assertTrue(r1.getSource().startsWith("10|"));
		assertTrue(r1.getTarget().startsWith("20|"));

		assertEquals(ModelConstants.DATASOURCE_ORGANIZATION, r1.getRelType());
		assertEquals(ModelConstants.DATASOURCE_ORGANIZATION, r2.getRelType());

		assertEquals(ModelConstants.PROVISION, r1.getSubRelType());
		assertEquals(ModelConstants.PROVISION, r2.getSubRelType());

		assertEquals(ModelConstants.IS_PROVIDED_BY, r1.getRelClass());
		assertEquals(ModelConstants.PROVIDES, r2.getRelClass());
	}

	@Test
	void testProcessProjectOrganization() throws Exception {
		final List<TypedField> fields = prepareMocks("projectorganization_resultset_entry.json");

		final List<Oaf> list = app.processProjectOrganization(rs);

		assertEquals(2, list.size());
		verifyMocks(fields);

		final Relation r1 = (Relation) list.get(0);
		final Relation r2 = (Relation) list.get(1);
		assertValidId(r1.getSource());
		assertValidId(r2.getSource());
		assertEquals(r1.getSource(), r2.getTarget());
		assertEquals(r2.getSource(), r1.getTarget());
		assertValidId(r1.getCollectedfrom().get(0).getKey());
		assertValidId(r2.getCollectedfrom().get(0).getKey());

		assertEquals(ModelConstants.PROJECT_ORGANIZATION, r1.getRelType());
		assertEquals(ModelConstants.PROJECT_ORGANIZATION, r2.getRelType());

		assertEquals(ModelConstants.PARTICIPATION, r1.getSubRelType());
		assertEquals(ModelConstants.PARTICIPATION, r2.getSubRelType());

		if (r1.getSource().startsWith("40")) {
			assertEquals(ModelConstants.HAS_PARTICIPANT, r1.getRelClass());
			assertEquals(ModelConstants.IS_PARTICIPANT, r2.getRelClass());
		} else if (r1.getSource().startsWith("20")) {
			assertEquals(ModelConstants.IS_PARTICIPANT, r1.getRelClass());
			assertEquals(ModelConstants.HAS_PARTICIPANT, r2.getRelClass());
		}

		assertNotNull(r1.getProperties());
		checkProperty(r1, "contribution", "436754.0");
		checkProperty(r2, "contribution", "436754.0");

		checkProperty(r1, "currency", "EUR");
		checkProperty(r2, "currency", "EUR");
	}

	private void checkProperty(Relation r, String property, String value) {
		final List<KeyValue> p = r
			.getProperties()
			.stream()
			.filter(kv -> kv.getKey().equals(property))
			.collect(Collectors.toList());
		assertFalse(p.isEmpty());
		assertEquals(1, p.size());
		assertEquals(value, p.get(0).getValue());
	}

	@Test
	public void testProcessClaims_context() throws Exception {
		final List<TypedField> fields = prepareMocks("claimscontext_resultset_entry.json");

		final List<Oaf> list = app.processClaims(rs);

		assertEquals(1, list.size());
		assertTrue(list.get(0) instanceof Result);
		final Result r = (Result) list.get(0);

		verifyMocks(fields);

		assertValidId(r.getCollectedfrom().get(0).getKey());
	}

	@Test
	void testProcessClaims_rels() throws Exception {
		final List<TypedField> fields = prepareMocks("claimsrel_resultset_entry.json");

		final List<Oaf> list = app.processClaims(rs);

		assertEquals(2, list.size());
		verifyMocks(fields);

		assertTrue(list.get(0) instanceof Relation);
		assertTrue(list.get(1) instanceof Relation);

		final Relation r1 = (Relation) list.get(0);
		final Relation r2 = (Relation) list.get(1);

		assertValidId(r1.getSource());
		assertValidId(r1.getTarget());
		assertValidId(r2.getSource());
		assertValidId(r2.getTarget());
		assertNotNull(r1.getDataInfo());
		assertNotNull(r2.getDataInfo());
		assertNotNull(r1.getDataInfo().getTrust());
		assertNotNull(r2.getDataInfo().getTrust());
		assertEquals(r1.getSource(), r2.getTarget());
		assertEquals(r2.getSource(), r1.getTarget());
		assertTrue(StringUtils.isNotBlank(r1.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r2.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r1.getRelType()));
		assertTrue(StringUtils.isNotBlank(r2.getRelType()));

		assertValidId(r1.getCollectedfrom().get(0).getKey());
		assertValidId(r2.getCollectedfrom().get(0).getKey());
	}

	@Test
	void testProcessClaims_affiliation() throws Exception {
		final List<TypedField> fields = prepareMocks("claimsrel_resultset_affiliation.json");

		final List<Oaf> list = app.processClaims(rs);

		assertEquals(2, list.size());
		verifyMocks(fields);

		assertTrue(list.get(0) instanceof Relation);
		assertTrue(list.get(1) instanceof Relation);

		final Relation r1 = (Relation) list.get(0);
		final Relation r2 = (Relation) list.get(1);

		assertValidId(r1.getSource());
		assertValidId(r1.getTarget());
		assertValidId(r2.getSource());
		assertValidId(r2.getTarget());
		assertNotNull(r1.getDataInfo());
		assertNotNull(r2.getDataInfo());
		assertNotNull(r1.getDataInfo().getTrust());
		assertNotNull(r2.getDataInfo().getTrust());
		assertEquals(r1.getSource(), r2.getTarget());
		assertEquals(r2.getSource(), r1.getTarget());
		assertTrue(StringUtils.isNotBlank(r1.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r2.getRelClass()));
		assertTrue(StringUtils.isNotBlank(r1.getRelType()));
		assertTrue(StringUtils.isNotBlank(r2.getRelType()));

		assertValidId(r1.getCollectedfrom().get(0).getKey());
		assertValidId(r2.getCollectedfrom().get(0).getKey());
	}

	private List<TypedField> prepareMocks(final String jsonFile) throws IOException, SQLException {
		final String json = IOUtils.toString(getClass().getResourceAsStream(jsonFile));
		final ObjectMapper mapper = new ObjectMapper();
		final List<TypedField> list = mapper.readValue(json, new TypeReference<List<TypedField>>() {
		});

		for (final TypedField tf : list) {
			if (tf.getValue() == null) {
				switch (tf.getType()) {
					case "not_used":
						break;
					case "boolean":
						Mockito.when(rs.getBoolean(tf.getField())).thenReturn(false);
						break;
					case "date":
						Mockito.when(rs.getDate(tf.getField())).thenReturn(null);
						break;
					case "int":
						Mockito.when(rs.getInt(tf.getField())).thenReturn(0);
						break;
					case "double":
						Mockito.when(rs.getDouble(tf.getField())).thenReturn(0.0);
						break;
					case "array":
						Mockito.when(rs.getArray(tf.getField())).thenReturn(null);
						break;
					case "string":
					default:
						Mockito.when(rs.getString(tf.getField())).thenReturn(null);
						break;
				}
			} else {
				switch (tf.getType()) {
					case "not_used":
						break;
					case "boolean":
						Mockito
							.when(rs.getBoolean(tf.getField()))
							.thenReturn(Boolean.parseBoolean(tf.getValue().toString()));
						break;
					case "date":
						Mockito
							.when(rs.getDate(tf.getField()))
							.thenReturn(Date.valueOf(tf.getValue().toString()));
						break;
					case "int":
						Mockito
							.when(rs.getInt(tf.getField()))
							.thenReturn(new Integer(tf.getValue().toString()));
						break;
					case "double":
						Mockito
							.when(rs.getDouble(tf.getField()))
							.thenReturn(new Double(tf.getValue().toString()));
						break;
					case "array":
						final Array arr = Mockito.mock(Array.class);
						final String[] values = ((List<?>) tf.getValue())
							.stream()
							.filter(Objects::nonNull)
							.map(Object::toString)
							.toArray(String[]::new);

						Mockito.when(arr.getArray()).thenReturn(values);
						Mockito.when(rs.getArray(tf.getField())).thenReturn(arr);
						break;
					case "string":
					default:
						Mockito.when(rs.getString(tf.getField())).thenReturn(tf.getValue().toString());
						break;
				}
			}
		}

		return list;
	}

	private void verifyMocks(final List<TypedField> list) throws SQLException {
		for (final TypedField tf : list) {

			switch (tf.getType()) {
				case "not_used":
					break;
				case "boolean":
					Mockito.verify(rs, Mockito.atLeastOnce()).getBoolean(tf.getField());
					break;
				case "date":
					Mockito.verify(rs, Mockito.atLeastOnce()).getDate(tf.getField());
					break;
				case "int":
					Mockito.verify(rs, Mockito.atLeastOnce()).getInt(tf.getField());
					break;
				case "double":
					Mockito.verify(rs, Mockito.atLeastOnce()).getDouble(tf.getField());
					break;
				case "array":
					Mockito.verify(rs, Mockito.atLeastOnce()).getArray(tf.getField());
					break;
				case "string":
				default:
					Mockito.verify(rs, Mockito.atLeastOnce()).getString(tf.getField());
					break;
			}
		}
	}

	private void assertValidId(final String id) {
		assertEquals(49, id.length());
		assertEquals('|', id.charAt(2));
		assertEquals(':', id.charAt(15));
		assertEquals(':', id.charAt(16));
	}

	private String getValueAsString(final String name, final List<TypedField> fields) {
		return getValueAs(name, fields);
	}

	private Float getValueAsFloat(final String name, final List<TypedField> fields) {
		final Object value = getValueAs(name, fields);
		return value != null ? new Float(value.toString()) : null;
	}

	private Double getValueAsDouble(final String name, final List<TypedField> fields) {
		final Object value = getValueAs(name, fields);
		return value != null ? new Double(value.toString()) : null;
	}

	private Integer getValueAsInt(final String name, final List<TypedField> fields) {
		final Object value = getValueAs(name, fields);
		return value != null ? new Integer(value.toString()) : null;
	}

	private <T> T getValueAs(final String name, final List<TypedField> fields) {
		final Optional<T> field = fields
			.stream()
			.filter(f -> f.getField().equals(name))
			.findFirst()
			.map(TypedField::getValue)
			.map(o -> (T) o);
		if (!field.isPresent()) {
			return null;
		}
		return field.get();
	}

	private List<String> getValueAsList(final String name, final List<TypedField> fields) {
		return getValueAs(name, fields);
	}
}

class TypedField {

	private String field;
	private String type;
	private Object value;

	public String getField() {
		return field;
	}

	public void setField(final String field) {
		this.field = field;
	}

	public String getType() {
		return type;
	}

	public void setType(final String type) {
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(final Object value) {
		this.value = value;
	}
}
