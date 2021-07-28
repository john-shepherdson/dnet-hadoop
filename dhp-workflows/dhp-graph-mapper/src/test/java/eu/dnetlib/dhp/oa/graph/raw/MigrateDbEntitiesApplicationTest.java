
package eu.dnetlib.dhp.oa.graph.raw;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

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
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

@ExtendWith(MockitoExtension.class)
public class MigrateDbEntitiesApplicationTest {

	private MigrateDbEntitiesApplication app;

	@Mock(lenient = true)
	private ResultSet rs;

	@Mock
	private VocabularyGroup vocs;

	@BeforeEach
	public void setUp() {
		lenient()
			.when(vocs.getTermAsQualifier(anyString(), anyString()))
			.thenAnswer(invocation -> OafMapperUtils
				.qualifier(invocation.getArgument(1), invocation.getArgument(1), invocation.getArgument(0), invocation.getArgument(0)));

		lenient().when(vocs.termExists(anyString(), anyString())).thenReturn(true);

		this.app = new MigrateDbEntitiesApplication(vocs);
	}

	@Test
	public void testProcessDatasource() throws Exception {
		final List<TypedField> fields = prepareMocks("datasources_resultset_entry.json");

		final List<Oaf> list = app.processDatasource(rs);
		assertEquals(1, list.size());
		verifyMocks(fields);

		final Datasource ds = (Datasource) list.get(0);
		assertValidId(ds.getId());
		assertValidId(ds.getCollectedfrom().get(0).getKey());
		assertEquals(getValueAsString("officialname", fields), ds.getOfficialname().getValue());
		assertEquals(getValueAsString("englishname", fields), ds.getEnglishname().getValue());
		assertEquals(getValueAsString("contactemail", fields), ds.getContactemail().getValue());
		assertEquals(getValueAsString("websiteurl", fields), ds.getWebsiteurl().getValue());
		assertEquals(getValueAsString("namespaceprefix", fields), ds.getNamespaceprefix().getValue());
		assertEquals(getValueAsString("collectedfromname", fields), ds.getCollectedfrom().get(0).getValue());
		assertEquals(getValueAsString("officialname", fields), ds.getJournal().getName());
		assertEquals(getValueAsString("issnPrinted", fields), ds.getJournal().getIssnPrinted());
		assertEquals(getValueAsString("issnOnline", fields), ds.getJournal().getIssnOnline());
		assertEquals(getValueAsString("issnLinking", fields), ds.getJournal().getIssnLinking());

		assertEquals("pubsrepository::journal", ds.getDatasourcetype().getClassid());
		assertEquals("dnet:datasource_typologies", ds.getDatasourcetype().getSchemeid());

		assertEquals("pubsrepository::journal", ds.getDatasourcetypeui().getClassid());
		assertEquals("dnet:datasource_typologies_ui", ds.getDatasourcetypeui().getSchemeid());

		assertEquals("National", ds.getJurisdiction().getClassid());
		assertEquals("eosc:jurisdictions", ds.getJurisdiction().getSchemeid());

		assertTrue(ds.getThematic());
		assertTrue(ds.getKnowledgegraph());

		assertEquals(1, ds.getContentpolicies().size());
		assertEquals("Journal article", ds.getContentpolicies().get(0).getClassid());
		assertEquals("eosc:contentpolicies", ds.getContentpolicies().get(0).getSchemeid());

	}

	@Test
	public void testProcessProject() throws Exception {
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
	public void testProcessOrganization() throws Exception {
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
	public void testProcessDatasourceOrganization() throws Exception {
		final List<TypedField> fields = prepareMocks("datasourceorganization_resultset_entry.json");

		final List<Oaf> list = app.processDatasourceOrganization(rs);

		assertEquals(2, list.size());
		verifyMocks(fields);

		final Relation r1 = (Relation) list.get(0);
		final Relation r2 = (Relation) list.get(1);
		assertValidId(r1.getSource());
		assertValidId(r2.getSource());
		assertEquals(r1.getSource(), r2.getTarget());
		assertEquals(r2.getSource(), r1.getTarget());
	}

	@Test
	public void testProcessProjectOrganization() throws Exception {
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
	public void testProcessClaims_rels() throws Exception {
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

		// System.out.println(new ObjectMapper().writeValueAsString(r1));
		// System.out.println(new ObjectMapper().writeValueAsString(r2));
	}

	private List<TypedField> prepareMocks(final String jsonFile) throws IOException, SQLException {
		final String json = IOUtils.toString(getClass().getResourceAsStream(jsonFile));
		final ObjectMapper mapper = new ObjectMapper();
		final List<TypedField> list = mapper.readValue(json, new TypeReference<List<TypedField>>() {});

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
						.map(o -> o.toString())
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
		return new Float(getValueAs(name, fields).toString());
	}

	private <T> T getValueAs(final String name, final List<TypedField> fields) {
		return fields
			.stream()
			.filter(f -> f.getField().equals(name))
			.map(TypedField::getValue)
			.filter(Objects::nonNull)
			.map(o -> (T) o)
			.findFirst()
			.get();
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
