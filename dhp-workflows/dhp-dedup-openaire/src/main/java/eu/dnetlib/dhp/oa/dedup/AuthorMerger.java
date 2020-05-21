package eu.dnetlib.dhp.oa.dedup;

import com.wcohen.ss.JaroWinkler;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.pace.model.Person;
import org.apache.commons.lang3.StringUtils;
import scala.Tuple2;

import java.text.Normalizer;
import java.util.*;
import java.util.stream.Collectors;

public class AuthorMerger {

    private static final Double THRESHOLD = 0.95;

    public static List<Author> merge(List<List<Author>> authors){

        authors.sort(new Comparator<List<Author>>() {
            @Override
            public int compare(List<Author> o1, List<Author> o2) {
                return -Integer.compare(countAuthorsPids(o1), countAuthorsPids(o2));
            }
        });

        List<Author> author = new ArrayList<>();

        for(List<Author> a : authors){
            author = mergeAuthor(author, a);
        }

        return author;

    }

    public static List<Author> mergeAuthor(final List<Author> a, final List<Author> b) {
        int pa = countAuthorsPids(a);
        int pb = countAuthorsPids(b);
        List<Author> base, enrich;
        int sa = authorsSize(a);
        int sb = authorsSize(b);

        if (pa == pb) {
            base = sa > sb ? a : b;
            enrich = sa > sb ? b : a;
        } else {
            base = pa > pb ? a : b;
            enrich = pa > pb ? b : a;
        }
        enrichPidFromList(base, enrich);
        return base;
    }

    private static void enrichPidFromList(List<Author> base, List<Author> enrich) {
        if (base == null || enrich == null)
            return;
        final Map<String, Author> basePidAuthorMap = base
                .stream()
                .filter(a -> a.getPid() != null && a.getPid().size() > 0)
                .flatMap(
                        a -> a
                                .getPid()
                                .stream()
                                .map(p -> new Tuple2<>(pidToComparableString(p), a)))
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2, (x1, x2) -> x1));

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
                            if (simAuthor.isPresent() && simAuthor.get()._1() > THRESHOLD) {
                                Author r = simAuthor.get()._2();
                                if (r.getPid() == null) {
                                    r.setPid(new ArrayList<>());
                                }
                                r.getPid().add(a._1());
                            }
                        });
    }

    public static String pidToComparableString(StructuredProperty pid){
        return (pid.getQualifier()!=null? pid.getQualifier().getClassid()!=null?pid.getQualifier().getClassid().toLowerCase():"":"") + (pid.getValue()!=null? pid.getValue().toLowerCase():"");
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

        if (pa.isAccurate() & pb.isAccurate()) {
            return new JaroWinkler()
                    .score(normalize(pa.getSurnameString()), normalize(pb.getSurnameString()));
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
        return nfd(s)
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
    }

    private static String nfd(final String s) {
        return Normalizer.normalize(s, Normalizer.Form.NFD);
    }

}
