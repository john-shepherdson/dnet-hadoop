
package eu.dnetlib.dhp.transformation.xslt;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PersonCleanerTest {

	private PersonCleaner personCleaner;

	@Before
	public void setup() {
		this.personCleaner = new PersonCleaner();
	}

	@Test
	public void shouldGetFirstname() {
		List<String> actualValue = personCleaner.getFirstname();

		// TODO: assert scenario
	}

	@Test
	public void shouldGetSurname() {
		List<String> actualValue = personCleaner.getSurname();

		// TODO: assert scenario
	}

	@Test
	public void shouldGetFullname() {
		List<String> actualValue = personCleaner.getFullname();

		// TODO: assert scenario
	}

	@Test
	public void shouldHash() {
		String actualValue = personCleaner.hash();

		// TODO: assert scenario
	}

	@Test
	public void shouldGetNormalisedFullname() {
		String actualValue = personCleaner.getNormalisedFullname();

		// TODO: assert scenario
	}

	@Test
	public void shouldGetCapitalSurname() {
		List<String> actualValue = personCleaner.getCapitalSurname();

		// TODO: assert scenario
	}

	@Test
	public void shouldGetNameWithAbbreviations() {
		List<String> actualValue = personCleaner.getNameWithAbbreviations();

		// TODO: assert scenario
	}

	@Test
	public void shouldIsAccurate() {
		boolean actualValue = personCleaner.isAccurate();

		// TODO: assert scenario
	}

	@Test
	public void shouldGetName() {
		QName actualValue = personCleaner.getName();

		// TODO: assert scenario
	}

	@Test
	public void shouldGetResultType() {
		SequenceType actualValue = personCleaner.getResultType();

		// TODO: assert scenario
	}

	@Test
	public void shouldGetArgumentTypes() {
		SequenceType[] actualValue = personCleaner.getArgumentTypes();

		// TODO: assert scenario
	}

	@Test
	public void shouldCall() {
		// TODO: initialize args
		XdmValue[] xdmValues;

		XdmValue actualValue = personCleaner.call(xdmValues);

		// TODO: assert scenario
	}
}
