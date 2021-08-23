
package eu.dnetlib.dhp.common;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.text.WordUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

/**
 * PacePerson tries to derive information from the fullname string of an author. Such informations are Names, Surnames
 * an Fullname split into terms. It provides also an additional field for the original data. The calculation of the
 * names and the surnames is not always possible. When it is impossible to assert which are the names and the surnames,
 * the lists are empty.
 */
public class PacePerson {

	private List<String> name = Lists.newArrayList();
	private List<String> surname = Lists.newArrayList();
	private List<String> fullname = Lists.newArrayList();
	private final String original;

	private static Set<String> particles = null;

	/**
	 * Capitalizes a string
	 *
	 * @param s the string to capitalize
	 * @return the input string with capital letter
	 */
	public static final String capitalize(final String s) {
		return WordUtils.capitalize(s.toLowerCase(), ' ', '-');
	}

	/**
	 * Adds a dot to a string with length equals to 1
	 */
	public static final String dotAbbreviations(final String s) {
		return s.length() == 1 ? s + "." : s;
	}

	public static Set<String> loadFromClasspath(final String classpath) {
		final Set<String> h = new HashSet<>();
		try {
			for (final String s : IOUtils.readLines(PacePerson.class.getResourceAsStream(classpath))) {
				h.add(s);
			}
		} catch (final Throwable e) {
			return new HashSet<>();
		}
		return h;
	}

	/**
	 * The constructor of the class. It fills the fields of the class basing on the input fullname.
	 *
	 * @param s the input string (fullname of the author)
	 * @param aggressive set the string normalization type
	 */
	public PacePerson(String s, final boolean aggressive) {
		original = s;
		s = Normalizer.normalize(s, Normalizer.Form.NFD);
		s = s.replaceAll("\\(.+\\)", "");
		s = s.replaceAll("\\[.+\\]", "");
		s = s.replaceAll("\\{.+\\}", "");
		s = s.replaceAll("\\s+-\\s+", "-");
		s = s.replaceAll("[\\p{Punct}&&[^,-]]", " ");
		s = s.replaceAll("\\d", " ");
		s = s.replaceAll("\\n", " ");
		s = s.replaceAll("\\.", " ");
		s = s.replaceAll("\\s+", " ");

		if (aggressive) {
			s = s.replaceAll("[\\p{InCombiningDiacriticalMarks}&&[^,-]]", "");
			// s = s.replaceAll("[\\W&&[^,-]]", "");
		}

		// if the string contains a comma, it can derive surname and name by splitting on it
		if (s.contains(",")) {
			final String[] arr = s.split(",");
			if (arr.length == 1) {
				fullname = splitTerms(arr[0]);
			} else if (arr.length > 1) {
				surname = splitTerms(arr[0]);
				name = splitTerms(arr[1]);
				fullname.addAll(surname);
				fullname.addAll(name);
			}
		} else { // otherwise, it should rely on CAPS terms and short terms
			fullname = splitTerms(s);

			int lastInitialPosition = fullname.size();
			boolean hasSurnameInUpperCase = false;

			// computes lastInitialPosition and hasSurnameInUpperCase
			for (int i = 0; i < fullname.size(); i++) {
				final String term = fullname.get(i);
				if (term.length() == 1) {
					lastInitialPosition = i; // first word in the name longer than 1 (to avoid name with dots)
				} else if (term.equals(term.toUpperCase())) {
					hasSurnameInUpperCase = true; // if one of the words is CAPS
				}
			}

			// manages particular cases of fullnames
			if (lastInitialPosition < fullname.size() - 1) { // Case: Michele G. Artini
				name = fullname.subList(0, lastInitialPosition + 1);
				surname = fullname.subList(lastInitialPosition + 1, fullname.size());
			} else if (hasSurnameInUpperCase) { // Case: Michele ARTINI
				for (final String term : fullname) {
					if (term.length() > 1 && term.equals(term.toUpperCase())) {
						surname.add(term);
					} else {
						name.add(term);
					}
				}
			}
		}
	}

	private List<String> splitTerms(final String s) {
		if (particles == null) {
			particles = loadFromClasspath("/eu/dnetlib/dhp/oa/graph/pace/name_particles.txt");
		}

		final List<String> list = Lists.newArrayList();
		for (final String part : Splitter.on(" ").omitEmptyStrings().split(s)) {
			if (!particles.contains(part.toLowerCase())) {
				list.add(part);
			}
		}
		return list;
	}

	public List<String> getName() {
		return name;
	}

	public String getNameString() {
		return Joiner.on(" ").join(getName());
	}

	public List<String> getSurname() {
		return surname;
	}

	public List<String> getFullname() {
		return fullname;
	}

	public String getOriginal() {
		return original;
	}

	public String hash() {
		return Hashing
			.murmur3_128()
			.hashString(getNormalisedFullname(), StandardCharsets.UTF_8)
			.toString();
	}

	public String getNormalisedFirstName() {
		return Joiner.on(" ").join(getCapitalFirstnames());
	}

	public String getNormalisedSurname() {
		return Joiner.on(" ").join(getCapitalSurname());
	}

	public String getSurnameString() {
		return Joiner.on(" ").join(getSurname());
	}

	public String getNormalisedFullname() {
		return isAccurate()
			? getNormalisedSurname() + ", " + getNormalisedFirstName()
			: Joiner.on(" ").join(fullname);
	}

	public List<String> getCapitalFirstnames() {
		return Lists
			.newArrayList(
				Iterables.transform(getNameWithAbbreviations(), PacePerson::capitalize));
	}

	public List<String> getCapitalSurname() {
		return Lists.newArrayList(Iterables.transform(surname, PacePerson::capitalize));
	}

	public List<String> getNameWithAbbreviations() {
		return Lists.newArrayList(Iterables.transform(name, PacePerson::dotAbbreviations));
	}

	public boolean isAccurate() {
		return name != null && surname != null && !name.isEmpty() && !surname.isEmpty();
	}
}
