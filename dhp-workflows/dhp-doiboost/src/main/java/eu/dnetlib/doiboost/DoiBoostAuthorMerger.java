
package eu.dnetlib.doiboost;

import java.text.Normalizer;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.wcohen.ss.JaroWinkler;

import eu.dnetlib.dhp.oa.merge.AuthorMerger;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.pace.model.Person;
import scala.Tuple2;

public class DoiBoostAuthorMerger {

	private static final Double THRESHOLD = 0.95;

	public static List<Author> merge(List<List<Author>> authors) {

		Iterator<List<Author>> it = authors.iterator();
		final List<Author> author = it.next();

		it.forEachRemaining(autList -> enrichPidFromList(author, autList, THRESHOLD));

		return author;

	}

	public static List<Author> mergeAuthor(final List<Author> crossrefAuthor, final List<Author> otherAuthor,
		Double threshold) {

		enrichPidFromList(crossrefAuthor, otherAuthor, threshold);
		return crossrefAuthor;
	}

	public static List<Author> mergeAuthor(final List<Author> crossrefAuthor, final List<Author> otherAuthor) {
		return mergeAuthor(crossrefAuthor, otherAuthor, THRESHOLD);
	}

	private static void enrichPidFromList(List<Author> base, List<Author> enrich, Double threshold) {
		if (base == null || enrich == null)
			return;

		// <pidComparableString, Author> (if an Author has more than 1 pid, it appears 2 times in the list)
		final Map<String, Author> basePidAuthorMap = base
			.stream()
			.filter(a -> a.getPid() != null && a.getPid().size() > 0)
			.flatMap(
				a -> a
					.getPid()
					.stream()
					.map(p -> new Tuple2<>(pidToComparableString(p), a)))
			.collect(Collectors.toMap(Tuple2::_1, Tuple2::_2, (x1, x2) -> x1));

		// <pid, Author> (list of pid that are missing in the other list)
		final List<Tuple2<StructuredProperty, Author>> pidToEnrich = enrich
			.stream()
			.filter(a -> a.getPid() != null && a.getPid().size() > 0)
			.flatMap(
				a -> a
					.getPid()
					.stream()
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

	public static String pidToComparableString(StructuredProperty pid) {
		return (pid.getQualifier() != null
			? pid.getQualifier().getClassid() != null ? pid.getQualifier().getClassid().toLowerCase() : ""
			: "")
			+ (pid.getValue() != null ? pid.getValue().toLowerCase() : "");
	}

	public static int countAuthorsPids(List<Author> authors) {
		if (authors == null)
			return 0;

		return (int) authors.stream().filter(DoiBoostAuthorMerger::hasPid).count();
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
		if (a == null || a.getPid() == null || a.getPid().size() == 0)
			return false;
		return a.getPid().stream().anyMatch(p -> p != null && StringUtils.isNotBlank(p.getValue()));
	}

	private static Person parse(Author author) {
		if (StringUtils.isNotBlank(author.getSurname())) {
			return new Person(author.getSurname() + ", " + author.getName(), false);
		} else {
			return new Person(author.getFullname(), false);
		}
	}

	private static String normalize(final String s) {
		String[] normalized = nfd(s)
			.replaceAll("[^\\p{ASCII}]", "")
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
