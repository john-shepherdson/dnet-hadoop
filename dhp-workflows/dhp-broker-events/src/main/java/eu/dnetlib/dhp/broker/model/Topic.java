
package eu.dnetlib.dhp.broker.model;

public enum Topic {

	// ENRICHMENT MISSING
	ENRICH_MISSING_OA_VERSION("ENRICH/MISSING/OPENACCESS_VERSION"), ENRICH_MISSING_ABSTRACT(
		"ENRICH/MISSING/ABSTRACT"), ENRICH_MISSING_PUBLICATION_DATE(
			"ENRICH/MISSING/PUBLICATION_DATE"), ENRICH_MISSING_PID(
				"ENRICH/MISSING/PID"), ENRICH_MISSING_PROJECT("ENRICH/MISSING/PROJECT"), ENRICH_MISSING_SOFTWARE(
					"ENRICH/MISSING/SOFTWARE"), ENRICH_MISSING_SUBJECT_MESHEUROPMC(
						"ENRICH/MISSING/SUBJECT/MESHEUROPMC"), ENRICH_MISSING_SUBJECT_ARXIV(
							"ENRICH/MISSING/SUBJECT/ARXIV"), ENRICH_MISSING_SUBJECT_JEL(
								"ENRICH/MISSING/SUBJECT/JEL"), ENRICH_MISSING_SUBJECT_DDC(
									"ENRICH/MISSING/SUBJECT/DDC"), ENRICH_MISSING_SUBJECT_ACM(
										"ENRICH/MISSING/SUBJECT/ACM"), ENRICH_MISSING_SUBJECT_RVK(
											"ENRICH/MISSING/SUBJECT/RVK"), ENRICH_MISSING_AUTHOR_ORCID(
												"ENRICH/MISSING/AUTHOR/ORCID"),

	// ENRICHMENT MORE
	ENRICH_MORE_PID("ENRICH/MORE/PID"), ENRICH_MORE_OA_VERSION("ENRICH/MORE/OPENACCESS_VERSION"), ENRICH_MORE_ABSTRACT(
		"ENRICH/MORE/ABSTRACT"), ENRICH_MORE_PUBLICATION_DATE("ENRICH/MORE/PUBLICATION_DATE"), ENRICH_MORE_PROJECT(
			"ENRICH/MORE/PROJECT"), ENRICH_MORE_SUBJECT_MESHEUROPMC(
				"ENRICH/MORE/SUBJECT/MESHEUROPMC"), ENRICH_MORE_SUBJECT_ARXIV(
					"ENRICH/MORE/SUBJECT/ARXIV"), ENRICH_MORE_SUBJECT_JEL(
						"ENRICH/MORE/SUBJECT/JEL"), ENRICH_MORE_SUBJECT_DDC(
							"ENRICH/MORE/SUBJECT/DDC"), ENRICH_MORE_SUBJECT_ACM(
								"ENRICH/MORE/SUBJECT/ACM"), ENRICH_MORE_SUBJECT_RVK("ENRICH/MORE/SUBJECT/RVK"),

	// ADDITION
	ADD_BY_PROJECT("ADD/BY_PROJECT");

	Topic(final String path) {
		this.path = path;
	}

	protected String path;

	public String getPath() {
		return this.path;
	}

	public static Topic fromPath(final String path) {
		for (final Topic t : Topic.values()) {
			if (t.getPath().equals(path)) {
				return t;
			}
		}
		return null;
	}

}
