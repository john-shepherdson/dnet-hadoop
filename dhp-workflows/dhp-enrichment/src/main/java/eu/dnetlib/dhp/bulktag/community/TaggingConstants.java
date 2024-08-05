
package eu.dnetlib.dhp.bulktag.community;

public class TaggingConstants {

	private TaggingConstants() {
	}

	public static final String BULKTAG_DATA_INFO_TYPE = "bulktagging";

	public static final String ANNOTATION_DATA_INFO_TYPE = "annotation";
	public static final String CLASS_ID_ANNOTATION = "graph:annotation";
	public static final String CLASS_NAME_ANNOTATION = "Graph Annotation";

	public static final String CLASS_ID_SUBJECT = "community:subject";
	public static final String CLASS_ID_DATASOURCE = "community:datasource";
	public static final String CLASS_ID_CZENODO = "community:zenodocommunity";
	public static final String CLASS_ID_ADVANCED_CONSTRAINT = "community:advconstraint";

	public static final String ZENODO_COMMUNITY_INDICATOR = "zenodo.org/communities/";

	public static final String CLASS_NAME_BULKTAG_SUBJECT = "Bulktagging for Community - Subject";
	public static final String CLASS_NAME_BULKTAG_DATASOURCE = "Bulktagging for Community - Datasource";
	public static final String CLASS_NAME_BULKTAG_ZENODO = "Bulktagging for Community - Zenodo";
	public static final String CLASS_NAME_BULKTAG_ADVANCED_CONSTRAINT = "Bulktagging for Community - Advanced Constraints";

	public static final String TAGGING_TRUST = "0.8";
}
