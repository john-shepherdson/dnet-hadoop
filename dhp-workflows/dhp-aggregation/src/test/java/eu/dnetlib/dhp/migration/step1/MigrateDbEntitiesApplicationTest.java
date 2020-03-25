package eu.dnetlib.dhp.migration.step1;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Oaf;

@RunWith(MockitoJUnitRunner.class)
public class MigrateDbEntitiesApplicationTest {

	private MigrateDbEntitiesApplication app;

	@Mock
	private ResultSet rs;

	@Before
	public void setUp() throws Exception {
		this.app = new MigrateDbEntitiesApplication();
	}

	@Test
	public void testProcessDatasource() throws Exception {
		final List<TypedField> fields = prepareMocks("datasources_resultset_entry.json");

		final List<Oaf> list = app.processDatasource(rs);
		assertEquals(1, list.size());

		verifyMocks(fields);
	}

	@Test
	public void testProcessProject() throws Exception {
		final List<TypedField> fields = prepareMocks("projects_resultset_entry.json");

		final List<Oaf> list = app.processProject(rs);
		assertEquals(1, list.size());

		verifyMocks(fields);
	}

	@Test
	public void testProcessOrganization() throws Exception {
		final List<TypedField> fields = prepareMocks("organizations_resultset_entry.json");

		final List<Oaf> list = app.processOrganization(rs);

		assertEquals(1, list.size());

		verifyMocks(fields);
	}

	@Test
	public void testProcessDatasourceOrganization() throws Exception {
		final List<TypedField> fields = prepareMocks("datasourceorganization_resultset_entry.json");

		final List<Oaf> list = app.processDatasourceOrganization(rs);

		assertEquals(2, list.size());
		verifyMocks(fields);
	}

	@Test
	public void testProcessProjectOrganization() throws Exception {
		final List<TypedField> fields = prepareMocks("projectorganization_resultset_entry.json");

		final List<Oaf> list = app.processProjectOrganization(rs);

		assertEquals(2, list.size());
		verifyMocks(fields);
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
