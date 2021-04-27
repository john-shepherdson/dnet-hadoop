
package eu.dnetlib.dhp.transformation.xslt;

import static eu.dnetlib.dhp.transformation.xslt.XSLTTransformationFunction.QNAME_BASE_URI;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

import eu.dnetlib.dhp.transformation.xslt.utils.Capitalize;
import eu.dnetlib.dhp.transformation.xslt.utils.DotAbbreviations;
import net.sf.saxon.s9api.*;

public class PersonCleaner implements ExtensionFunction, Serializable {

	private static final long serialVersionUID = 1L;
	private List<String> firstname = Lists.newArrayList();
	private List<String> surname = Lists.newArrayList();
	private List<String> fullname = Lists.newArrayList();

	private static Set<String> particles = null;

	public PersonCleaner() {

	}

	private String normalize(String s) {
		s = Normalizer.normalize(s, Normalizer.Form.NFD); // was NFD
		s = s.replaceAll("\\(.+\\)", "");
		s = s.replaceAll("\\[.+\\]", "");
		s = s.replaceAll("\\{.+\\}", "");
		s = s.replaceAll("\\s+-\\s+", "-");

//              s = s.replaceAll("[\\W&&[^,-]]", " ");

//              System.out.println("class Person: s: " + s);

//              s = s.replaceAll("[\\p{InCombiningDiacriticalMarks}&&[^,-]]", " ");
		s = s.replaceAll("[\\p{Punct}&&[^-,]]", " ");
		s = s.replace("\\d", " ");
		s = s.replace("\\n", " ");
		s = s.replace("\\.", " ");
		s = s.replaceAll("\\s+", " ");

		if (s.contains(",")) {
			// System.out.println("class Person: s: " + s);

			String[] arr = s.split(",");
			if (arr.length == 1) {

				fullname = splitTerms(arr[0]);
			} else if (arr.length > 1) {
				surname = splitTerms(arr[0]);
				firstname = splitTermsFirstName(arr[1]);
//                              System.out.println("class Person: surname: " + surname);
//                              System.out.println("class Person: firstname: " + firstname);

				fullname.addAll(surname);
				fullname.addAll(firstname);
			}
		} else {
			fullname = splitTerms(s);

			int lastInitialPosition = fullname.size();
			boolean hasSurnameInUpperCase = false;

			for (int i = 0; i < fullname.size(); i++) {
				String term = fullname.get(i);
				if (term.length() == 1) {
					lastInitialPosition = i;
				} else if (term.equals(term.toUpperCase())) {
					hasSurnameInUpperCase = true;
				}
			}
			if (lastInitialPosition < fullname.size() - 1) { // Case: Michele G. Artini
				firstname = fullname.subList(0, lastInitialPosition + 1);
				System.out.println("name: " + firstname);
				surname = fullname.subList(lastInitialPosition + 1, fullname.size());
			} else if (hasSurnameInUpperCase) { // Case: Michele ARTINI
				for (String term : fullname) {
					if (term.length() > 1 && term.equals(term.toUpperCase())) {
						surname.add(term);
					} else {
						firstname.add(term);
					}
				}
			} else if (lastInitialPosition == fullname.size()) {
				surname = fullname.subList(lastInitialPosition - 1, fullname.size());
				firstname = fullname.subList(0, lastInitialPosition - 1);
			}

		}
		return null;
	}

	private List<String> splitTermsFirstName(String s) {
		List<String> list = Lists.newArrayList();
		for (String part : Splitter.on(" ").omitEmptyStrings().split(s)) {
			if (s.trim().matches("\\p{Lu}{2,3}")) {
				String[] parts = s.trim().split("(?=\\p{Lu})"); // (Unicode UpperCase)
				for (String p : parts) {
					if (p.length() > 0)
						list.add(p);
				}
			} else {
				list.add(part);
			}

		}
		return list;
	}

	private List<String> splitTerms(String s) {
		if (particles == null) {
			// particles = NGramUtils.loadFromClasspath("/eu/dnetlib/pace/config/name_particles.txt");
		}

		List<String> list = Lists.newArrayList();
		for (String part : Splitter.on(" ").omitEmptyStrings().split(s)) {
			// if (!particles.contains(part.toLowerCase())) {
			list.add(part);

			// }
		}
		return list;
	}

	public List<String> getFirstname() {
		return firstname;
	}

	public List<String> getSurname() {
		return surname;
	}

	public List<String> getFullname() {
		return fullname;
	}

	public String hash() {
		return Hashing.murmur3_128().hashString(getNormalisedFullname(), StandardCharsets.UTF_8).toString();
	}

	public String getNormalisedFullname() {
		return isAccurate() ? Joiner.on(" ").join(getSurname()) + ", " + Joiner.on(" ").join(getNameWithAbbreviations())
			: Joiner.on(" ").join(fullname);
		// return isAccurate() ?
		// Joiner.on(" ").join(getCapitalSurname()) + ", " + Joiner.on(" ").join(getNameWithAbbreviations()) :
		// Joiner.on(" ").join(fullname);
	}

	public List<String> getCapitalSurname() {
		return Lists.newArrayList(Iterables.transform(surname, new Capitalize()));
	}

	public List<String> getNameWithAbbreviations() {
		return Lists.newArrayList(Iterables.transform(firstname, new DotAbbreviations()));
	}

	public boolean isAccurate() {
		return (firstname != null && surname != null && !firstname.isEmpty() && !surname.isEmpty());
	}

	@Override
	public QName getName() {
		return new QName(QNAME_BASE_URI + "/person", "normalize");
	}

	@Override
	public SequenceType getResultType() {
		return SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ZERO_OR_ONE);
	}

	@Override
	public SequenceType[] getArgumentTypes() {
		return new SequenceType[] {
			SequenceType.makeSequenceType(ItemType.STRING, OccurrenceIndicator.ZERO_OR_ONE)
		};
	}

	@Override
	public XdmValue call(XdmValue[] xdmValues) throws SaxonApiException {
		XdmValue r = xdmValues[0];
		if (r.size() == 0) {
			return new XdmAtomicValue("");
		}
		final String currentValue = xdmValues[0].itemAt(0).getStringValue();
		return new XdmAtomicValue(normalize(currentValue));
	}
}
