
package eu.dnetlib.dhp.oa.graph.merge;

import java.util.Comparator;

import eu.dnetlib.dhp.schema.oaf.Qualifier;

public class DatasourceCompatibilityComparator implements Comparator<Qualifier> {
	@Override
	public int compare(Qualifier left, Qualifier right) {

		String lClass = left.getClassid();
		String rClass = right.getClassid();

		if (lClass.equals(rClass))
			return 0;

		if (lClass.equals("openaire-cris_1.1"))
			return -1;
		if (rClass.equals("openaire-cris_1.1"))
			return 1;

		if (lClass.equals("openaire4.0"))
			return -1;
		if (rClass.equals("openaire4.0"))
			return 1;

		if (lClass.equals("driver-openaire2.0"))
			return -1;
		if (rClass.equals("driver-openaire2.0"))
			return 1;

		if (lClass.equals("driver"))
			return -1;
		if (rClass.equals("driver"))
			return 1;

		if (lClass.equals("openaire2.0"))
			return -1;
		if (rClass.equals("openaire2.0"))
			return 1;

		if (lClass.equals("openaire3.0"))
			return -1;
		if (rClass.equals("openaire3.0"))
			return 1;

		if (lClass.equals("openaire2.0_data"))
			return -1;
		if (rClass.equals("openaire2.0_data"))
			return 1;

		if (lClass.equals("native"))
			return -1;
		if (rClass.equals("native"))
			return 1;

		if (lClass.equals("hostedBy"))
			return -1;
		if (rClass.equals("hostedBy"))
			return 1;

		if (lClass.equals("notCompatible"))
			return -1;
		if (rClass.equals("notCompatible"))
			return 1;

		if (lClass.equals("UNKNOWN"))
			return -1;
		if (rClass.equals("UNKNOWN"))
			return 1;

		// Else (but unlikely), lexicographical ordering will do.
		return lClass.compareTo(rClass);
	}

	/*
	 * CASE WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility):: TEXT) @> ARRAY
	 * ['openaire-cris_1.1']) THEN 'openaire-cris_1.1@@@dnet:datasourceCompatibilityLevel' WHEN (array_agg(DISTINCT
	 * COALESCE (a.compatibility_override, a.compatibility):: TEXT) @> ARRAY ['openaire4.0']) THEN
	 * 'openaire4.0@@@dnet:datasourceCompatibilityLevel' WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override,
	 * a.compatibility):: TEXT) @> ARRAY ['driver', 'openaire2.0']) THEN
	 * 'driver-openaire2.0@@@dnet:datasourceCompatibilityLevel' WHEN (array_agg(DISTINCT COALESCE
	 * (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['driver']) THEN
	 * 'driver@@@dnet:datasourceCompatibilityLevel' WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override,
	 * a.compatibility) :: TEXT) @> ARRAY ['openaire2.0']) THEN 'openaire2.0@@@dnet:datasourceCompatibilityLevel' WHEN
	 * (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['openaire3.0']) THEN
	 * 'openaire3.0@@@dnet:datasourceCompatibilityLevel' WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override,
	 * a.compatibility) :: TEXT) @> ARRAY ['openaire2.0_data']) THEN
	 * 'openaire2.0_data@@@dnet:datasourceCompatibilityLevel' WHEN (array_agg(DISTINCT COALESCE
	 * (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['native']) THEN
	 * 'native@@@dnet:datasourceCompatibilityLevel' WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override,
	 * a.compatibility) :: TEXT) @> ARRAY ['hostedBy']) THEN 'hostedBy@@@dnet:datasourceCompatibilityLevel' WHEN
	 * (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['notCompatible'])
	 * THEN 'notCompatible@@@dnet:datasourceCompatibilityLevel' ELSE 'UNKNOWN@@@dnet:datasourceCompatibilityLevel' END
	 */
}
