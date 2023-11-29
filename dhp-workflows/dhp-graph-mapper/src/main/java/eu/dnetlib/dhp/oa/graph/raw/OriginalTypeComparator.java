
package eu.dnetlib.dhp.oa.graph.raw;

import static org.apache.commons.lang3.StringUtils.contains;
import static org.apache.commons.lang3.StringUtils.startsWith;

import java.util.Comparator;

public class OriginalTypeComparator implements Comparator<String> {

	@Override
	public int compare(String t1, String t2) {

		if (t1.equals(t2)) {
			return 0;
		}
		if (startsWith(t1, "http") && contains(t1, "coar") && contains(t1, "resource_type")) {
			return -1;
		}
		if (startsWith(t2, "http") && contains(t2, "coar") && contains(t2, "resource_type")) {
			return 1;
		}
		if (startsWith(t1, "info:eu-repo/semantics")) {
			return -1;
		}
		if (startsWith(t2, "info:eu-repo/semantics")) {
			return 1;
		}

		return t1.compareTo(t2);
	}

}
