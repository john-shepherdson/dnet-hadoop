
package eu.dnetlib.dhp.oa.graph.raw.common;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Splitter;

import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Relation;

/**
 * This predicate should be used to skip oaf objects using a blacklist of nsprefixes.
 *
 * @author michele
 */
public class VerifyNsPrefixPredicate implements Predicate<Oaf> {

	final Set<String> invalids = new HashSet<>();

	public VerifyNsPrefixPredicate(final String blacklist) {
		if (StringUtils.isNotBlank(blacklist)) {
			Splitter
				.on(",")
				.trimResults()
				.omitEmptyStrings()
				.split(blacklist)
				.forEach(invalids::add);
		}
	}

	@Override
	public boolean test(final Oaf oaf) {
		if (oaf instanceof Datasource) {
			return testValue(((Datasource) oaf).getNamespaceprefix().getValue());
		} else if (oaf instanceof OafEntity) {
			return testValue(((OafEntity) oaf).getId());
		} else if (oaf instanceof Relation) {
			return testValue(((Relation) oaf).getSource()) && testValue(((Relation) oaf).getTarget());
		} else {
			return true;
		}
	}

	protected boolean testValue(final String s) {
		if (StringUtils.isNotBlank(s)) {
			for (final String invalid : invalids) {
				if (Pattern.matches("^(\\d\\d\\|)?" + invalid + ".*$", s)) {
					return false;
				}
			}
		}
		return true;
	}

}
