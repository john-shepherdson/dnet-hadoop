
package eu.dnetlib.dhp.schema.oaf.utils;

public class ModelHardLimits {

	private ModelHardLimits() {
	}

	public static final String LAYOUT = "index";
	public static final String INTERPRETATION = "openaire";
	public static final String SEPARATOR = "-";

	public static final int MAX_EXTERNAL_ENTITIES = 50;
	public static final int MAX_AUTHORS = 200;
	public static final int MAX_AUTHOR_FULLNAME_LENGTH = 1000;
	public static final int MAX_TITLE_LENGTH = 5000;
	public static final int MAX_TITLES = 10;
	public static final int MAX_ABSTRACTS = 10;
	public static final int MAX_ABSTRACT_LENGTH = 150000;
	public static final int MAX_RELATED_ABSTRACT_LENGTH = 500;
	public static final int MAX_INSTANCES = 10;

	public static String getCollectionName(String format) {
		return format + SEPARATOR + LAYOUT + SEPARATOR + INTERPRETATION;
	}

}
