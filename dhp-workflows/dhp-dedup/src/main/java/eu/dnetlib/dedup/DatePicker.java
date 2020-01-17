package eu.dnetlib.dedup;

import eu.dnetlib.dhp.schema.oaf.Field;
import org.apache.commons.lang.StringUtils;

import java.time.Year;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.reverseOrder;
import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang.StringUtils.endsWith;
import static org.apache.commons.lang.StringUtils.substringBefore;

public class DatePicker {

    private static final String DATE_PATTERN = "\\d{4}-\\d{2}-\\d{2}";
    private static final String DATE_DEFAULT_SUFFIX = "01-01";
    private static final int YEAR_LB = 1300;
    private static final int YEAR_UB = Year.now().getValue() + 5;

    public static Field<String> pick(final Collection<String> dateofacceptance) {

        final Map<String, Integer> frequencies = dateofacceptance
                .parallelStream()
                .filter(StringUtils::isNotBlank)
                .collect(
                        Collectors.toConcurrentMap(
                                w -> w, w -> 1, Integer::sum));

        if (frequencies.isEmpty()) {
            return new Field<>();
        }

        final Field<String> date = new Field<>();
                date.setValue(frequencies.keySet().iterator().next());

        // let's sort this map by values first, filtering out invalid dates
        final Map<String, Integer> sorted = frequencies
                .entrySet()
                .stream()
                .filter(d -> StringUtils.isNotBlank(d.getKey()))
                .filter(d -> d.getKey().matches(DATE_PATTERN))
                .filter(d -> inRange(d.getKey()))
                .sorted(reverseOrder(comparingByValue()))
                .collect(
                        toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue, (e1, e2) -> e2,
                                LinkedHashMap::new));

        // shortcut
        if (sorted.size() == 0) {
            return date;
        }

        // voting method (1/3 + 1) wins
        if (sorted.size() >= 3) {
            final int acceptThreshold = (sorted.size() / 3) + 1;
            final List<String> accepted = sorted.entrySet().stream()
                    .filter(e -> e.getValue() >= acceptThreshold)
                    .map(e -> e.getKey())
                    .collect(Collectors.toList());

            // cannot find strong majority
            if (accepted.isEmpty()) {
                final int max = sorted.values().iterator().next();
                Optional<String> first = sorted.entrySet().stream()
                        .filter(e -> e.getValue() == max && !endsWith(e.getKey(), DATE_DEFAULT_SUFFIX))
                        .map(Map.Entry::getKey)
                        .findFirst();
                if (first.isPresent()) {
                    date.setValue(first.get());
                    return date;
                }

                date.setValue(sorted.keySet().iterator().next());
                return date;
            }

            if (accepted.size() == 1) {
                date.setValue(accepted.get(0));
                return date;
            } else {
                final Optional<String> first = accepted.stream()
                        .filter(d -> !endsWith(d, DATE_DEFAULT_SUFFIX))
                        .findFirst();
                if (first.isPresent()) {
                    date.setValue(first.get());
                    return date;
                }

                return date;
            }

            //1st non YYYY-01-01 is returned
        } else {
            if (sorted.size() == 2) {
                for (Map.Entry<String, Integer> e : sorted.entrySet()) {
                    if (!endsWith(e.getKey(), DATE_DEFAULT_SUFFIX)) {
                        date.setValue(e.getKey());
                        return date;
                    }
                }
            }

            // none of the dates seems good enough, return the 1st one
            date.setValue(sorted.keySet().iterator().next());
            return date;
        }
    }

    private static boolean inRange(final String date) {
        final int year = Integer.parseInt(substringBefore(date, "-"));
        return year >= YEAR_LB && year <= YEAR_UB;
    }

}