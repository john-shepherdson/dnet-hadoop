
package eu.dnetlib.pace.model;

import java.util.Comparator;

import org.apache.spark.sql.Row;

import eu.dnetlib.pace.clustering.NGramUtils;

/**
 * The Class MapDocumentComparator.
 */
public class RowDataOrderingComparator implements Comparator<Row> {

	/** The comparator field. */
	private final int comparatorField;
	private final int identityFieldPosition;

	/**
	 * Instantiates a new map document comparator.
	 *
	 * @param comparatorField
	 *            the comparator field
	 */
	public RowDataOrderingComparator(final int comparatorField, int identityFieldPosition) {
		this.comparatorField = comparatorField;
		this.identityFieldPosition = identityFieldPosition;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(final Row d1, final Row d2) {
		if (d1 == null)
			return d2 == null ? 0 : -1;
		else if (d2 == null) {
			return 1;
		}

		final String o1 = d1.getString(comparatorField);
		final String o2 = d2.getString(comparatorField);

		if (o1 == null)
			return o2 == null ? 0 : -1;
		else if (o2 == null) {
			return 1;
		}

		final String to1 = NGramUtils.cleanupForOrdering(o1);
		final String to2 = NGramUtils.cleanupForOrdering(o2);

		int res = to1.compareTo(to2);
		if (res == 0) {
			res = o1.compareTo(o2);
			if (res == 0) {
				return d1.getString(identityFieldPosition).compareTo(d2.getString(identityFieldPosition));
			}
		}

		return res;
	}

}
