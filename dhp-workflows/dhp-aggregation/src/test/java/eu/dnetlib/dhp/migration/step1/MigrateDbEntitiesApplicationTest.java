package eu.dnetlib.dhp.migration.step1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;

@RunWith(MockitoJUnitRunner.class)
public class MigrateDbEntitiesApplicationTest {

	private MigrateDbEntitiesApplication app;

	@Mock
	private ResultSet rs;

	@BeforeEach
	public void setUp() throws Exception {
		this.app = new MigrateDbEntitiesApplication();
	}

	@Test
	public void testProcessDatasource() throws Exception {
		final List<TypedField> fields = prepareMocks("datasources_resultset_entry.json");

		final List<Oaf> list = app.processDatasource(rs);
		assertEquals(1, list.size());
		verifyMocks(fields);

		final Datasource ds = (Datasource) list.get(0);
		assertValidId(ds.getId());
		assertEquals(ds.getOfficialname().getValue(), getValueAsString("officialname", fields));
		assertEquals(ds.getEnglishname().getValue(), getValueAsString("englishname", fields));
		assertEquals(ds.getContactemail().getValue(), getValueAsString("contactemail", fields));
		assertEquals(ds.getWebsiteurl().getValue(), getValueAsString("websiteurl", fields));
		assertEquals(ds.getNamespaceprefix().getValue(), getValueAsString("namespaceprefix", fields));
		assertEquals(ds.getCollectedfrom().get(0).getKey(), getValueAsString("collectedfromid", fields));
		assertEquals(ds.getCollectedfrom().get(0).getValue(), getValueAsString("collectedfromname", fields));
	}

	@Test
	public void testProcessProject() throws Exception {
		final List<TypedField> fields = prepareMocks("projects_resultset_entry.json");

		final List<Oaf> list = app.processProject(rs);
		assertEquals(1, list.size());
		verifyMocks(fields);

		final Project p = (Project) list.get(0);
		assertValidId(p.getId());
		assertEquals(p.getAcronym().getValue(), getValueAsString("acronym", fields));
		assertEquals(p.getTitle().getValue(), getValueAsString("title", fields));
		assertEquals(p.getCollectedfrom().get(0).getKey(), getValueAsString("collectedfromid", fields));
		assertEquals(p.getCollectedfrom().get(0).getValue(), getValueAsString("collectedfromname", fields));
	}

	@Test
	public void testProcessOrganization() throws Exception {
		final List<TypedField> fields = prepareMocks("organizations_resultset_entry.json");

		final List<Oaf> list = app.processOrganization(rs);

		assertEquals(1, list.size());

		verifyMocks(fields);

		final Organization o = (Organization) list.get(0);
		assertValidId(o.getId());
		assertEquals(o.getLegalshortname().getValue(), getValueAsString("legalshortname", fields));
		assertEquals(o.getLegalname().getValue(), getValueAsString("legalname", fields));
		assertEquals(o.getWebsiteurl().getValue(), getValueAsString("websiteurl", fields));
		assertEquals(o.getCountry().getClassid(), getValueAsString("country", fields).split("@@@")[0]);
		assertEquals(o.getCountry().getClassname(), getValueAsString("country", fields).split("@@@")[1]);
		assertEquals(o.getCountry().getSchemeid(), getValueAsString("country", fields).split("@@@")[2]);
		assertEquals(o.getCountry().getSchemename(), getValueAsString("country", fields).split("@@@")[3]);
		assertEquals(o.getCollectedfrom().get(0).getKey(), getValueAsString("collectedfromid", fields));
		assertEquals(o.getCollectedfrom().get(0).getValue(), getValueAsString("collectedfromname", fields));
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
	}

	@Test
	public void testProcessClaims_context() throws Exception {
		final List<TypedField> fields = prepareMocks("claimscontext_resultset_entry.json");

		final List<Oaf> list = app.processClaims(rs);

		assertEquals(1, list.size());
		verifyMocks(fields);
	}

	@Test
	public void testProcessClaims_rels() throws Exception {
		final List<TypedField> fields = prepareMocks("claimsrel_resultset_entry.json");

		final List<Oaf> list = app.processClaims(rs);

		assertEquals(2, list.size());
		verifyMocks(fields);
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
					Mockito.when(rs.getBoolean(tf.getField())).thenReturn(Boolean.parseBoolean(tf.getValue().toString()));
					break;
				case "date":
					Mockito.when(rs.getDate(tf.getField())).thenReturn(Date.valueOf(tf.getValue().toString()));
					break;
				case "int":
					Mockito.when(rs.getInt(tf.getField())).thenReturn(new Integer(tf.getValue().toString()));
					break;
				case "double":
					Mockito.when(rs.getDouble(tf.getField())).thenReturn(new Double(tf.getValue().toString()));
					break;
				case "array":
					final Array arr = Mockito.mock(Array.class);
					final String[] values = ((List<?>) tf.getValue()).stream()
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
		return fields.stream()
				.filter(f -> f.getField().equals(name))
				.map(TypedField::getValue)
				.filter(Objects::nonNull)
				.map(o -> o.toString())
				.findFirst()
				.get();
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
