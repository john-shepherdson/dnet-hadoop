package eu.dnetlib.dhp.oa.dedup;

import com.google.common.collect.Sets;
import com.wcohen.ss.JaroWinkler;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.clustering.BlacklistAwareClusteringCombiner;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.model.Person;
import java.io.StringReader;
import java.security.MessageDigest;
import java.text.Normalizer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import scala.Tuple2;

public class DedupUtility {
    private static final Double THRESHOLD = 0.95;

    public static Map<String, LongAccumulator> constructAccumulator(
            final DedupConfig dedupConf, final SparkContext context) {

        Map<String, LongAccumulator> accumulators = new HashMap<>();

        String acc1 =
                String.format(
                        "%s::%s", dedupConf.getWf().getEntityType(), "records per hash key = 1");
        accumulators.put(acc1, context.longAccumulator(acc1));
        String acc2 =
                String.format(
                        "%s::%s",
                        dedupConf.getWf().getEntityType(),
                        "missing " + dedupConf.getWf().getOrderField());
        accumulators.put(acc2, context.longAccumulator(acc2));
        String acc3 =
                String.format(
                        "%s::%s",
                        dedupConf.getWf().getEntityType(),
                        String.format(
                                "Skipped records for count(%s) >= %s",
                                dedupConf.getWf().getOrderField(),
                                dedupConf.getWf().getGroupMaxSize()));
        accumulators.put(acc3, context.longAccumulator(acc3));
        String acc4 = String.format("%s::%s", dedupConf.getWf().getEntityType(), "skip list");
        accumulators.put(acc4, context.longAccumulator(acc4));
        String acc5 =
                String.format("%s::%s", dedupConf.getWf().getEntityType(), "dedupSimilarity (x2)");
        accumulators.put(acc5, context.longAccumulator(acc5));
        String acc6 =
                String.format(
                        "%s::%s",
                        dedupConf.getWf().getEntityType(),
                        "d < " + dedupConf.getWf().getThreshold());
        accumulators.put(acc6, context.longAccumulator(acc6));

        return accumulators;
    }

    static Set<String> getGroupingKeys(DedupConfig conf, MapDocument doc) {
        return Sets.newHashSet(BlacklistAwareClusteringCombiner.filterAndCombine(doc, conf));
    }

    public static String md5(final String s) {
        try {
            final MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.getBytes("UTF-8"));
            return new String(Hex.encodeHex(md.digest()));
        } catch (final Exception e) {
            System.err.println("Error creating id");
            return null;
        }
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
        if (base == null || enrich == null) return;
        final Map<String, Author> basePidAuthorMap =
                base.stream()
                        .filter(a -> a.getPid() != null && a.getPid().size() > 0)
                        .flatMap(
                                a ->
                                        a.getPid().stream()
                                                .map(p -> new Tuple2<>(p.toComparableString(), a)))
                        .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2, (x1, x2) -> x1));

        final List<Tuple2<StructuredProperty, Author>> pidToEnrich =
                enrich.stream()
                        .filter(a -> a.getPid() != null && a.getPid().size() > 0)
                        .flatMap(
                                a ->
                                        a.getPid().stream()
                                                .filter(
                                                        p ->
                                                                !basePidAuthorMap.containsKey(
                                                                        p.toComparableString()))
                                                .map(p -> new Tuple2<>(p, a)))
                        .collect(Collectors.toList());

        pidToEnrich.forEach(
                a -> {
                    Optional<Tuple2<Double, Author>> simAuhtor =
                            base.stream()
                                    .map(ba -> new Tuple2<>(sim(ba, a._2()), ba))
                                    .max(Comparator.comparing(Tuple2::_1));
                    if (simAuhtor.isPresent() && simAuhtor.get()._1() > THRESHOLD) {
                        Author r = simAuhtor.get()._2();
                        r.getPid().add(a._1());
                    }
                });
    }

    public static String createDedupRecordPath(
            final String basePath, final String actionSetId, final String entityType) {
        return String.format("%s/%s/%s_deduprecord", basePath, actionSetId, entityType);
    }

    public static String createEntityPath(final String basePath, final String entityType) {
        return String.format("%s/%s", basePath, entityType);
    }

    public static String createSimRelPath(
            final String basePath, final String actionSetId, final String entityType) {
        return String.format("%s/%s/%s_simrel", basePath, actionSetId, entityType);
    }

    public static String createMergeRelPath(
            final String basePath, final String actionSetId, final String entityType) {
        return String.format("%s/%s/%s_mergerel", basePath, actionSetId, entityType);
    }

    private static Double sim(Author a, Author b) {

        final Person pa = parse(a);
        final Person pb = parse(b);

        if (pa.isAccurate() & pb.isAccurate()) {
            return new JaroWinkler()
                    .score(normalize(pa.getSurnameString()), normalize(pb.getSurnameString()));
        } else {
            return new JaroWinkler()
                    .score(
                            normalize(pa.getNormalisedFullname()),
                            normalize(pb.getNormalisedFullname()));
        }
    }

    private static String normalize(final String s) {
        return nfd(s).toLowerCase()
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

    private static Person parse(Author author) {
        if (StringUtils.isNotBlank(author.getSurname())) {
            return new Person(author.getSurname() + ", " + author.getName(), false);
        } else {
            return new Person(author.getFullname(), false);
        }
    }

    private static int countAuthorsPids(List<Author> authors) {
        if (authors == null) return 0;

        return (int) authors.stream().filter(DedupUtility::hasPid).count();
    }

    private static int authorsSize(List<Author> authors) {
        if (authors == null) return 0;
        return authors.size();
    }

    private static boolean hasPid(Author a) {
        if (a == null || a.getPid() == null || a.getPid().size() == 0) return false;
        return a.getPid().stream().anyMatch(p -> p != null && StringUtils.isNotBlank(p.getValue()));
    }

    public static List<DedupConfig> getConfigurations(String isLookUpUrl, String orchestrator)
            throws ISLookUpException, DocumentException {
        final ISLookUpService isLookUpService = ISLookupClientFactory.getLookUpService(isLookUpUrl);

        final String xquery =
                String.format(
                        "/RESOURCE_PROFILE[.//DEDUPLICATION/ACTION_SET/@id = '%s']", orchestrator);

        String orchestratorProfile = isLookUpService.getResourceProfileByQuery(xquery);

        final Document doc = new SAXReader().read(new StringReader(orchestratorProfile));

        final String actionSetId = doc.valueOf("//DEDUPLICATION/ACTION_SET/@id");
        final List<DedupConfig> configurations = new ArrayList<>();

        for (final Object o : doc.selectNodes("//SCAN_SEQUENCE/SCAN")) {
            configurations.add(loadConfig(isLookUpService, actionSetId, o));
        }

        return configurations;
    }

    private static DedupConfig loadConfig(
            final ISLookUpService isLookUpService, final String actionSetId, final Object o)
            throws ISLookUpException {
        final Element s = (Element) o;
        final String configProfileId = s.attributeValue("id");
        final String conf =
                isLookUpService.getResourceProfileByQuery(
                        String.format(
                                "for $x in /RESOURCE_PROFILE[.//RESOURCE_IDENTIFIER/@value = '%s'] return $x//DEDUPLICATION/text()",
                                configProfileId));
        final DedupConfig dedupConfig = DedupConfig.load(conf);
        dedupConfig.getWf().setConfigurationId(actionSetId);
        return dedupConfig;
    }
}
