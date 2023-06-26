
package eu.dnetlib.pace.tree.support;

public enum MatchType {

	MATCH, NO_MATCH, UNDEFINED;

	public static MatchType parse(String value) {

		if (MATCH.name().equals(value)) {
			return MATCH;
		} else if (NO_MATCH.name().equals(value)) {
			return NO_MATCH;
		} else {
			return UNDEFINED;
		}

//        try {
//            return MatchType.valueOf(value);
//        }
//        catch (IllegalArgumentException e) {
//            return MatchType.UNDEFINED; //return UNDEFINED if the enum is not parsable
//        }
	}
}
