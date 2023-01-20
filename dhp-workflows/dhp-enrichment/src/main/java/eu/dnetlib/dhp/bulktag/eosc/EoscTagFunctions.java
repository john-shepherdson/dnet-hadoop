
package eu.dnetlib.dhp.bulktag.eosc;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

import java.util.*;

import static eu.dnetlib.dhp.bulktag.community.TaggingConstants.*;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PROVENANCE_ACTIONS;

public class EoscTagFunctions {

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	public static final String EOSC_GALAXY_WORKFLOW = "EOSC::Galaxy Workflow";
	public static final String EOSC_TWITTER_DATA = "EOSC::Twitter Data";
	public static final String EOSC_JUPYTER_NOTEBOOK = "EOSC::Jupyter Notebook";
	public static final String COMPLIES_WITH = "compliesWith";

	public static <R extends Result> R enrich(R value, List<String> hostedByList) {
		if (value
				.getInstance()
				.stream()
				.anyMatch(
						i -> (hostedByList.contains(i.getHostedby().getKey())))
				&&
				!value.getContext().stream().anyMatch(c -> c.getId().equals("eosc"))) {
			Context context = new Context();
			context.setId("eosc");
			context
					.setDataInfo(
							Arrays
									.asList(
											OafMapperUtils
													.dataInfo(
															false, BULKTAG_DATA_INFO_TYPE, true, false,
															OafMapperUtils
																	.qualifier(
																			CLASS_ID_DATASOURCE, CLASS_NAME_BULKTAG_DATASOURCE,
																			DNET_PROVENANCE_ACTIONS, DNET_PROVENANCE_ACTIONS),
															TAGGING_TRUST)));
			value.getContext().add(context);

		}
		return value;
	}

	public static <R extends Result> R execEoscTagResult(R result) {
		if (result instanceof Software) {
			Software s = (Software) result;
			if (containsCriteriaNotebook(s)) {
				if (!Optional.ofNullable(s.getEoscifguidelines()).isPresent())
					s.setEoscifguidelines(new ArrayList<>());
				addEIG(
						s.getEoscifguidelines(), EOSC_JUPYTER_NOTEBOOK, EOSC_JUPYTER_NOTEBOOK, "",
						COMPLIES_WITH);
			}
			if (containsCriteriaGalaxy(s)) {
				if (!Optional.ofNullable(s.getEoscifguidelines()).isPresent())
					s.setEoscifguidelines(new ArrayList<>());

				addEIG(
						s.getEoscifguidelines(), EOSC_GALAXY_WORKFLOW, EOSC_GALAXY_WORKFLOW, "", COMPLIES_WITH);
			}
			return (R) s;

		} else if (result instanceof OtherResearchProduct) {
			OtherResearchProduct orp = (OtherResearchProduct) result;

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
			return (R) orp;
		} else if (result instanceof Dataset) {
			Dataset d = (Dataset) result;

			if (!Optional.ofNullable(d.getEoscifguidelines()).isPresent())
				d.setEoscifguidelines(new ArrayList<>());
			if (containscriteriaTwitter(d)) {
				addEIG(d.getEoscifguidelines(), EOSC_TWITTER_DATA, EOSC_TWITTER_DATA, "", COMPLIES_WITH);
			}
			return (R) d;
		}

		// this is a Publication, return it as it is.
		return result;
	}

	private static void addEIG(List<EoscIfGuidelines> eoscifguidelines, String code, String label, String url,
		String sem) {
		if (!eoscifguidelines.stream().anyMatch(eig -> eig.getCode().equals(code)))
			eoscifguidelines.add(newInstance(code, label, url, sem));
	}

	public static EoscIfGuidelines newInstance(String code, String label, String url, String semantics) {
		EoscIfGuidelines eig = new EoscIfGuidelines();
		eig.setCode(code);
		eig.setLabel(label);
		eig.setUrl(url);
		eig.setSemanticRelation(semantics);
		return eig;
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

	private static boolean containsCriteriaNotebook(Software s) {
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
