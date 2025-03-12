
package eu.openaire.common.author;

import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class TestAuthor {

	private final String fullName;

	public TestAuthor(String fullName) {
		this.fullName = fullName;
	}

	public static List<TestAuthor> of(String... fullNames) {
		return Arrays.stream(fullNames).map(TestAuthor::new).collect(Collectors.toList());
	}

	public String getFullName() {
		return fullName;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this, NO_CLASS_NAME_STYLE)
			.append("fullName", fullName)
			.toString();
	}
}
