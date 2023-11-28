
package eu.dnetlib.dhp.oa.merge;

import java.io.FileWriter;
import java.io.IOException;
import java.text.Normalizer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import com.wcohen.ss.JaroWinkler;

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.pace.model.Person;
import scala.Tuple2;

class SimilarityCellInfo implements Comparable<SimilarityCellInfo> {

	public int authorPosition = 0;
	public int orcidPosition = 0;

	public double maxColumnSimilarity = 0.0;

	public SimilarityCellInfo() {
	}

	public void setValues(final int authPos, final int orcidPos, final double similarity) {
		this.authorPosition = authPos;
		this.orcidPosition = orcidPos;
		this.maxColumnSimilarity = similarity;
	}

	@Override
	public int compareTo(@NotNull SimilarityCellInfo o) {
		return Double.compare(maxColumnSimilarity, o.maxColumnSimilarity);
	}
}

public class AuthorMerger {

	private static final Double THRESHOLD = 0.95;

	private AuthorMerger() {
	}

	public static List<Author> merge(List<List<Author>> authors) {

		authors.sort((o1, o2) -> -Integer.compare(countAuthorsPids(o1), countAuthorsPids(o2)));

		List<Author> author = new ArrayList<>();

		for (List<Author> a : authors) {
			author = mergeAuthor(author, a);
		}

		return author;

	}

	public static List<Author> mergeAuthor(final List<Author> a, final List<Author> b, Double threshold) {
		int pa = countAuthorsPids(a);
		int pb = countAuthorsPids(b);
		List<Author> base;
		List<Author> enrich;
		int sa = authorsSize(a);
		int sb = authorsSize(b);

		if (sa == sb) {
			base = pa > pb ? a : b;
			enrich = pa > pb ? b : a;
		} else {
			base = sa > sb ? a : b;
			enrich = sa > sb ? b : a;
		}
		enrichPidFromList(base, enrich, threshold);
		return base;
	}

	public static List<Author> mergeAuthor(final List<Author> a, final List<Author> b) {
		return mergeAuthor(a, b, THRESHOLD);
	}

	private static void enrichPidFromList(List<Author> base, List<Author> enrich, Double threshold) {
		if (base == null || enrich == null)
			return;

		// <pidComparableString, Author> (if an Author has more than 1 pid, it appears 2 times in the list)
		final Map<String, Author> basePidAuthorMap = base
			.stream()
			.filter(a -> a.getPid() != null && !a.getPid().isEmpty())
			.flatMap(
				a -> a
					.getPid()
					.stream()
					.filter(Objects::nonNull)
					.map(p -> new Tuple2<>(pidToComparableString(p), a)))
			.collect(Collectors.toMap(Tuple2::_1, Tuple2::_2, (x1, x2) -> x1));

		// <pid, Author> (list of pid that are missing in the other list)
		final List<Tuple2<StructuredProperty, Author>> pidToEnrich = enrich
			.stream()
			.filter(a -> a.getPid() != null && !a.getPid().isEmpty())
			.flatMap(
				a -> a
					.getPid()
					.stream()
					.filter(Objects::nonNull)
					.filter(p -> !basePidAuthorMap.containsKey(pidToComparableString(p)))
					.map(p -> new Tuple2<>(p, a)))
			.collect(Collectors.toList());

		pidToEnrich
			.forEach(
				a -> {
					Optional<Tuple2<Double, Author>> simAuthor = base
						.stream()
						.map(ba -> new Tuple2<>(sim(ba, a._2()), ba))
						.max(Comparator.comparing(Tuple2::_1));

					if (simAuthor.isPresent()) {
						double th = threshold;
						// increase the threshold if the surname is too short
						if (simAuthor.get()._2().getSurname() != null
							&& simAuthor.get()._2().getSurname().length() <= 3 && threshold > 0.0)
							th = 0.99;

						if (simAuthor.get()._1() > th) {
							Author r = simAuthor.get()._2();
							if (r.getPid() == null) {
								r.setPid(new ArrayList<>());
							}

							// TERRIBLE HACK but for some reason when we create and Array with Arrays.asList,
							// it creates of fixed size, and the add method raise UnsupportedOperationException at
							// java.util.AbstractList.add
							final List<StructuredProperty> tmp = new ArrayList<>(r.getPid());
							tmp.add(a._1());
							r.setPid(tmp);
						}
					}
				});
	}

	public static String normalizeFullName(final String fullname) {
		return nfd(fullname)
			.toLowerCase()
			// do not compact the regexes in a single expression, would cause StackOverflowError
			// in case
			// of large input strings
			.replaceAll("(\\W)+", " ")
			.replaceAll("(\\p{InCombiningDiacriticalMarks})+", " ")
			.replaceAll("(\\p{Punct})+", " ")
			.replaceAll("(\\d)+", " ")
			.replaceAll("(\\n)+", " ")

			.trim();
//        return Arrays.stream(fullname.split("[\\s | , | ;]+")).map(String::toLowerCase).sorted().collect(Collectors.joining());
	}

//
//    public static List<Author> enrichOrcid2(List<Author> baseAuthor, List<Author> orcidAuthor) {
//        if (baseAuthor == null || baseAuthor.isEmpty())
//            return orcidAuthor;
//
//        if (orcidAuthor == null || orcidAuthor.isEmpty())
//            return baseAuthor;
//
//        if (baseAuthor.size() == 1 && orcidAuthor.size() > 10)
//            return baseAuthor;
//
//
//        Map<String, List<Author>> pubClusters = baseAuthor.stream().collect(Collectors.toMap(AuthorMerger::generateAuthorkey, Arrays::asList, (a, b) -> {
//            a.addAll(b);
//            return a;
//        }));
//
//        Map<String, List<Author>> orcidClusters = baseAuthor.stream().collect(Collectors.toMap(AuthorMerger::generateAuthorkey, Arrays::asList, (a, b) -> {
//            a.addAll(b);
//            return a;
//        }));
//
//        System.out.println(pubClusters.keySet().size());
//        System.out.println(orcidClusters.keySet().size());
//
//
//
//
//       return null;
//
//
//    }

	static int hammingDist(String str1, String str2) {
		if (str1.length() != str2.length())
			return Math.max(str1.length(), str2.length());
		int i = 0, count = 0;
		while (i < str1.length()) {
			if (str1.charAt(i) != str2.charAt(i))
				count++;
			i++;
		}
		return count;
	}

	private static String authorFieldToBeCompared(Author author) {
		if (StringUtils.isNotBlank(author.getSurname())) {
			return author.getSurname();

		}
		if (StringUtils.isNotBlank(author.getFullname())) {
			return author.getFullname();
		}
		return null;
	}

	public static boolean checkSimilarity2(final Author left, final Author right) {
		final Person pl = parse(left);
		final Person pr = parse(right);

		// If one of them didn't have a surname we verify if they have the fullName not empty
		// and verify if the normalized version is equal
		if (!(pl.getSurname() != null && pl.getSurname().stream().anyMatch(StringUtils::isNotBlank) &&
			pr.getSurname() != null && pr.getSurname().stream().anyMatch(StringUtils::isNotBlank))) {

			if (pl.getFullname() != null && !pl.getFullname().isEmpty() && pr.getFullname() != null
				&& !pr.getFullname().isEmpty()) {
				return pl
					.getFullname()
					.stream()
					.anyMatch(
						fl -> pr.getFullname().stream().anyMatch(fr -> normalize(fl).equalsIgnoreCase(normalize(fr))));
			} else {
				return false;
			}
		}
		// The Authors have one surname in common
		if (pl.getSurname().stream().anyMatch(sl -> pr.getSurname().stream().anyMatch(sr -> sr.equalsIgnoreCase(sl)))) {

			// If one of them has only a surname and is the same we can say that they are the same author
			if ((pl.getName() == null || pl.getName().stream().allMatch(StringUtils::isBlank)) ||
				(pr.getName() == null || pr.getName().stream().allMatch(StringUtils::isBlank)))
				return true;
			// The authors have the same initials of Name in common
			if (pl
				.getName()
				.stream()
				.anyMatch(
					nl -> pr
						.getName()
						.stream()
						.anyMatch(nr -> nr.substring(0, 1).equalsIgnoreCase(nl.substring(0, 1)))))
				return true;
		}

		// Sometimes we noticed that publication have author wrote in inverse order Surname, Name
		// We verify if we have an exact match between name and surname
		if (pl.getSurname().stream().anyMatch(sl -> pr.getName().stream().anyMatch(nr -> nr.equalsIgnoreCase(sl))) &&
			pl.getName().stream().anyMatch(nl -> pr.getSurname().stream().anyMatch(sr -> sr.equalsIgnoreCase(nl))))
			return true;
		else
			return false;
	}

	public static List<Author> enrichOrcid2(List<Author> baseAuthor, List<Author> orcidAuthor) {

		if (baseAuthor == null || baseAuthor.isEmpty())
			return orcidAuthor;

		if (orcidAuthor == null || orcidAuthor.isEmpty())
			return baseAuthor;

		if (baseAuthor.size() == 1 && orcidAuthor.size() > 10)
			return baseAuthor;

		final List<Author> oAuthor = new ArrayList<>();
		oAuthor.addAll(orcidAuthor);

		baseAuthor.forEach(ba -> {
			Optional<Author> aMatch = oAuthor.stream().filter(oa -> checkSimilarity2(ba, oa)).findFirst();
			if (aMatch.isPresent()) {
				final Author sameAuthor = aMatch.get();
				addPid(ba, sameAuthor.getPid());
				oAuthor.remove(sameAuthor);
			}
		});
		return baseAuthor;
	}

	public static List<Author> enrichOrcid(List<Author> baseAuthor, List<Author> orcidAuthor) {

		if (baseAuthor == null || baseAuthor.isEmpty())
			return orcidAuthor;

		if (orcidAuthor == null || orcidAuthor.isEmpty())
			return baseAuthor;

		if (baseAuthor.size() == 1 && orcidAuthor.size() > 10)
			return baseAuthor;

		final Double similarityMatrix[][] = new Double[baseAuthor.size()][orcidAuthor.size()];

		final List<SimilarityCellInfo> maxColums = new ArrayList<>();

		for (int i = 0; i < orcidAuthor.size(); i++)
			maxColums.add(new SimilarityCellInfo());

		for (int i = 0; i < baseAuthor.size(); i++) {
			for (int j = 0; j < orcidAuthor.size(); j++) {
				similarityMatrix[i][j] = sim(baseAuthor.get(i), orcidAuthor.get(j));
				if (maxColums.get(j).maxColumnSimilarity < similarityMatrix[i][j])
					maxColums.get(j).setValues(i, j, similarityMatrix[i][j]);
			}
		}
		maxColums
			.stream()
			.sorted()
			.filter(si -> si.maxColumnSimilarity > 0.85)
			.forEach(si -> addPid(baseAuthor.get(si.authorPosition), orcidAuthor.get(si.orcidPosition).getPid()));
		return baseAuthor;

	}

	private static void addPid(final Author a, final List<StructuredProperty> pids) {

		if (a.getPid() == null) {
			a.setPid(new ArrayList<>());
		}

		a.getPid().addAll(pids);

	}

	public static String pidToComparableString(StructuredProperty pid) {
		final String classid = pid.getQualifier().getClassid() != null ? pid.getQualifier().getClassid().toLowerCase()
			: "";
		return (pid.getQualifier() != null ? classid : "")
			+ (pid.getValue() != null ? pid.getValue().toLowerCase() : "");
	}

	public static int countAuthorsPids(List<Author> authors) {
		if (authors == null)
			return 0;

		return (int) authors.stream().filter(AuthorMerger::hasPid).count();
	}

	private static int authorsSize(List<Author> authors) {
		if (authors == null)
			return 0;
		return authors.size();
	}

	private static Double sim(Author a, Author b) {

		final Person pa = parse(a);
		final Person pb = parse(b);

		// if both are accurate (e.g. they have name and surname)
		if (pa.isAccurate() & pb.isAccurate()) {
			return new JaroWinkler().score(normalize(pa.getSurnameString()), normalize(pb.getSurnameString())) * 0.5
				+ new JaroWinkler().score(normalize(pa.getNameString()), normalize(pb.getNameString())) * 0.5;
		} else {
			return new JaroWinkler()
				.score(normalize(pa.getNormalisedFullname()), normalize(pb.getNormalisedFullname()));
		}
	}

	private static boolean hasPid(Author a) {
		if (a == null || a.getPid() == null || a.getPid().isEmpty())
			return false;
		return a.getPid().stream().anyMatch(p -> p != null && StringUtils.isNotBlank(p.getValue()));
	}

	private static Person parse(Author author) {
		if (StringUtils.isNotBlank(author.getSurname())) {
			return new Person(author.getSurname() + ", " + author.getName(), false);
		} else {
			if (StringUtils.isNotBlank(author.getFullname()))
				return new Person(author.getFullname(), false);
			else
				return new Person("", false);
		}
	}

	public static String normalize(final String s) {
		String[] normalized = nfd(s)
			.toLowerCase()
			// do not compact the regexes in a single expression, would cause StackOverflowError
			// in case
			// of large input strings
			.replaceAll("(\\W)+", " ")
			.replaceAll("(\\p{InCombiningDiacriticalMarks})+", " ")
			.replaceAll("(\\p{Punct})+", " ")
			.replaceAll("(\\d)+", " ")
			.replaceAll("(\\n)+", " ")
			.trim()
			.split(" ");

		Arrays.sort(normalized);

		return String.join(" ", normalized);
	}

	private static String nfd(final String s) {
		return Normalizer.normalize(s, Normalizer.Form.NFD);
	}

}
