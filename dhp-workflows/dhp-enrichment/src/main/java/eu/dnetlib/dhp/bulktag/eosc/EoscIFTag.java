
package eu.dnetlib.dhp.bulktag.eosc;

import java.util.*;

import eu.dnetlib.dhp.schema.oaf.*;

public class EoscIFTag {

	public static final String EOSC_GALAXY_WORKFLOW = "EOSC::Galaxy Workflow";
	public static final String EOSC_TWITTER_DATA = "EOSC::Twitter Data";
	public static final String EOSC_JUPYTER_NOTEBOOK = "EOSC::Jupyter Notebook";
	public static final String COMPLIES_WITH = "compliesWith";

	public static EoscIfGuidelines newInstance(String code, String label, String url, String semantics) {
		EoscIfGuidelines eig = new EoscIfGuidelines();
		eig.setCode(code);
		eig.setLabel(label);
		eig.setUrl(url);
		eig.setSemanticRelation(semantics);
		return eig;

	}

	public static <R extends Result> void tagForSoftware(R s) {
		if (!Optional.ofNullable(s.getEoscifguidelines()).isPresent())
			s.setEoscifguidelines(new ArrayList<>());
		if (containsCriteriaNotebook(s)) {
			addEIG(
				s.getEoscifguidelines(), EOSC_JUPYTER_NOTEBOOK, EOSC_JUPYTER_NOTEBOOK, "",
				COMPLIES_WITH);
		}
		if (containsCriteriaGalaxy(s)) {
			addEIG(
				s.getEoscifguidelines(), EOSC_GALAXY_WORKFLOW, EOSC_GALAXY_WORKFLOW, "", COMPLIES_WITH);
		}

	}

	public static <R extends Result> void tagForOther(R orp) {
		if (!Optional.ofNullable(orp.getEoscifguidelines()).isPresent())
			orp.setEoscifguidelines(new ArrayList<>());

		if (containsCriteriaGalaxy(orp)) {
			addEIG(
				orp.getEoscifguidelines(), EOSC_GALAXY_WORKFLOW, EOSC_GALAXY_WORKFLOW, "",
				COMPLIES_WITH);
		}
		if (containscriteriaTwitter(orp)) {
			addEIG(orp.getEoscifguidelines(), EOSC_TWITTER_DATA, EOSC_TWITTER_DATA, "", COMPLIES_WITH);
		}

	}

	public static <R extends Result> void tagForDataset(R d) {
		if (!Optional.ofNullable(d.getEoscifguidelines()).isPresent())
			d.setEoscifguidelines(new ArrayList<>());
		if (containscriteriaTwitter(d)) {
			addEIG(d.getEoscifguidelines(), EOSC_TWITTER_DATA, EOSC_TWITTER_DATA, "", COMPLIES_WITH);
		}

	}

	private static void addEIG(List<EoscIfGuidelines> eoscifguidelines, String code, String label, String url,
		String sem) {
		if (!eoscifguidelines.stream().anyMatch(eig -> eig.getCode().equals(code)))
			eoscifguidelines.add(newInstance(code, label, url, sem));
	}

	private static boolean containscriteriaTwitter(Result r) {
		Set<String> words = getWordsSP(r.getTitle());
		words.addAll(getWordsF(r.getDescription()));

		if (words.contains("twitter") &&
			(words.contains("data") || words.contains("dataset")))
			return true;

		return Optional
			.ofNullable(r.getSubject())
			.map(
				s -> s.stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("twitter")) &&
					s.stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("data")))
			.orElse(false);
	}

	private static boolean containsCriteriaGalaxy(Result r) {
		Set<String> words = getWordsSP(r.getTitle());
		words.addAll(getWordsF(r.getDescription()));
		if (words.contains("galaxy") &&
			words.contains("workflow"))
			return true;

		return Optional
			.ofNullable(r.getSubject())
			.map(
				s -> s.stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("galaxy")) &&
					s.stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("workflow")))
			.orElse(false);
	}

	private static <R extends Result> boolean containsCriteriaNotebook(R s) {
		if (!Optional.ofNullable(s.getSubject()).isPresent())
			return false;
		if (s.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("jupyter")))
			return true;
		if (s
			.getSubject()
			.stream()
			.anyMatch(
				sbj -> sbj.getValue().toLowerCase().contains("python") &&
					sbj.getValue().toLowerCase().contains("notebook")))
			return true;
		if (s.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("python")) &&
			s.getSubject().stream().anyMatch(sbj -> sbj.getValue().toLowerCase().contains("notebook")))
			return true;
		return false;
	}

	private static Set<String> getWordsSP(List<StructuredProperty> elem) {
		Set<String> words = new HashSet<>();
		Optional
			.ofNullable(elem)
			.ifPresent(
				e -> e
					.forEach(
						t -> words
							.addAll(
								Arrays.asList(t.getValue().toLowerCase().replaceAll("[^a-zA-Z ]", "").split(" ")))));
		return words;
	}

	private static Set<String> getWordsF(List<Field<String>> elem) {
		Set<String> words = new HashSet<>();
		Optional
			.ofNullable(elem)
			.ifPresent(
				e -> e
					.forEach(
						t -> words
							.addAll(
								Arrays.asList(t.getValue().toLowerCase().replaceAll("[^a-zA-Z ]", "").split(" ")))));

		return words;
	}
}
