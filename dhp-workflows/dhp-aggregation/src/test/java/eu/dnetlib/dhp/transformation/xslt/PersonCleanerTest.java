
package eu.dnetlib.dhp.transformation.xslt;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PersonCleanerTest {

	private PersonCleaner personCleaner;

	@BeforeEach
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
}
