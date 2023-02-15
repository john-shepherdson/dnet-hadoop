
package eu.dnetlib.dhp.bulktag.community;

import static eu.dnetlib.dhp.bulktag.community.TaggingConstants.*;
import static eu.dnetlib.dhp.schema.common.ModelConstants.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

/** Created by miriam on 02/08/2018. */
public class ResultTagger implements Serializable {

	private boolean clearContext(Result result) {
		int tmp = result.getContext().size();
		List<Context> clist = result
			.getContext()
			.stream()
			.filter(c -> (!c.getId().contains(ZENODO_COMMUNITY_INDICATOR)))
			.collect(Collectors.toList());
		result.setContext(clist);
		return (tmp != clist.size());
	}

	private Map<String, List<String>> getParamMap(final Result result, Map<String, String> params) {
		Map<String, List<String>> param = new HashMap<>();
		String json = new Gson().toJson(result, Result.class);
		DocumentContext jsonContext = JsonPath.parse(json);
		if (params == null) {
			params = new HashMap<>();
		}
		for (String key : params.keySet()) {
			try {
				param.put(key, jsonContext.read(params.get(key)));
			} catch (com.jayway.jsonpath.PathNotFoundException e) {
				param.put(key, new ArrayList<>());
			}
		}
		return param;
	}

	public <R extends Result> R enrichContextCriteria(
		final R result, final CommunityConfiguration conf, final Map<String, String> criteria) {

		final Map<String, List<String>> param = getParamMap(result, criteria);

		// Verify if the entity is deletedbyinference. In case verify if to clean the context list
		// from all the zenodo communities
		if (result.getDataInfo().getDeletedbyinference()) {
			clearContext(result);
			return result;
		}

		// communities contains all the communities to be added as context for the result
		final Set<String> communities = new HashSet<>();

		// tagging for Subject
		final Set<String> subjects = new HashSet<>();

		if (Objects.nonNull(result.getSubject())) {
			result
				.getSubject()
				.stream()
				.map(StructuredProperty::getValue)
				.filter(StringUtils::isNotBlank)
				.map(String::toLowerCase)
				.map(String::trim)
				.collect(Collectors.toCollection(HashSet::new))
				.forEach(s -> subjects.addAll(conf.getCommunityForSubjectValue(s)));
		}

		communities.addAll(subjects);

		// Tagging for datasource
		final Set<String> datasources = new HashSet<>();
		final Set<String> tmp = new HashSet<>();

		if (Objects.nonNull(result.getInstance())) {
			for (Instance i : result.getInstance()) {
				if (Objects.nonNull(i.getCollectedfrom()) && Objects.nonNull(i.getCollectedfrom().getKey())) {
					tmp.add(StringUtils.substringAfter(i.getCollectedfrom().getKey(), "|"));
				}
				if (Objects.nonNull(i.getHostedby()) && Objects.nonNull(i.getHostedby().getKey())) {
					tmp.add(StringUtils.substringAfter(i.getHostedby().getKey(), "|"));
				}

			}

			tmp
				.forEach(
					dsId -> datasources
						.addAll(
							conf.getCommunityForDatasource(dsId, param)));
		}

		communities.addAll(datasources);

		/* Tagging for Zenodo Communities */
		final Set<String> czenodo = new HashSet<>();

		Optional<List<Context>> oresultcontext = Optional.ofNullable(result.getContext());
		if (oresultcontext.isPresent()) {
			oresultcontext
				.get()
				.stream()
				.filter(c -> c.getId().contains(ZENODO_COMMUNITY_INDICATOR))
				.collect(Collectors.toList())
				.forEach(
					c -> czenodo
						.addAll(
							conf
								.getCommunityForZenodoCommunityValue(
									c
										.getId()
										.substring(
											c.getId().lastIndexOf("/") + 1)
										.trim())));
		}

		communities.addAll(czenodo);

		/* Tagging for Advanced Constraints */
		final Set<String> aconstraints = new HashSet<>();

		conf
			.getSelectionConstraintsMap()
			.keySet()
			.forEach(communityId -> {
				if (conf.getSelectionConstraintsMap().get(communityId) != null &&
					conf
						.getSelectionConstraintsMap()
						.get(communityId)
						.getCriteria()
						.stream()
						.anyMatch(crit -> crit.verifyCriteria(param)))
					aconstraints.add(communityId);
			});

		communities.addAll(aconstraints);

		clearContext(result);

		/* Verify if there is something to bulktag */
		if (communities.isEmpty()) {
			return result;
		}

		result.getContext().forEach(c -> {
			final String cId = c.getId();
			if (communities.contains(cId)) {
				Optional<List<DataInfo>> opt_dataInfoList = Optional.ofNullable(c.getDataInfo());
				List<DataInfo> dataInfoList;
				if (opt_dataInfoList.isPresent())
					dataInfoList = opt_dataInfoList.get();
				else {
					dataInfoList = new ArrayList<>();
					c.setDataInfo(dataInfoList);
				}
				if (subjects.contains(cId))
					dataInfoList
						.add(
							OafMapperUtils
								.dataInfo(
									TAGGING_TRUST, BULKTAG_DATA_INFO_TYPE, true,
									OafMapperUtils
										.qualifier(
											CLASS_ID_SUBJECT, CLASS_NAME_BULKTAG_SUBJECT, DNET_PROVENANCE_ACTIONS)));
				if (datasources.contains(cId))
					dataInfoList
						.add(
							OafMapperUtils
								.dataInfo(
									TAGGING_TRUST, BULKTAG_DATA_INFO_TYPE, true,
									OafMapperUtils
										.qualifier(
											CLASS_ID_DATASOURCE, CLASS_NAME_BULKTAG_DATASOURCE,
											DNET_PROVENANCE_ACTIONS)));
				if (czenodo.contains(cId))
					dataInfoList
						.add(
							OafMapperUtils
								.dataInfo(
									TAGGING_TRUST, BULKTAG_DATA_INFO_TYPE, true,
									OafMapperUtils
										.qualifier(
											CLASS_ID_CZENODO, CLASS_NAME_BULKTAG_ZENODO, DNET_PROVENANCE_ACTIONS)));
				if (aconstraints.contains(cId))
					dataInfoList
						.add(
							OafMapperUtils
								.dataInfo(
									TAGGING_TRUST, BULKTAG_DATA_INFO_TYPE, true,
									OafMapperUtils
										.qualifier(
											CLASS_ID_ADVANCED_CONSTRAINT, CLASS_NAME_BULKTAG_ADVANCED_CONSTRAINT,
											DNET_PROVENANCE_ACTIONS)));

			}
		});

		communities
			.removeAll(
				result.getContext().stream().map(Context::getId).collect(Collectors.toSet()));

		if (communities.isEmpty())
			return result;

		List<Context> toaddcontext = communities
			.stream()
			.map(
				c -> {
					Context context = new Context();
					context.setId(c);
					List<DataInfo> dataInfoList = new ArrayList<>();
					if (subjects.contains(c))
						dataInfoList
							.add(
								OafMapperUtils
									.dataInfo(
										TAGGING_TRUST, BULKTAG_DATA_INFO_TYPE, true,
										OafMapperUtils
											.qualifier(
												CLASS_ID_SUBJECT, CLASS_NAME_BULKTAG_SUBJECT,
												DNET_PROVENANCE_ACTIONS)));
					if (datasources.contains(c))
						dataInfoList
							.add(
								OafMapperUtils
									.dataInfo(
										TAGGING_TRUST, BULKTAG_DATA_INFO_TYPE, true,
										OafMapperUtils
											.qualifier(
												CLASS_ID_DATASOURCE, CLASS_NAME_BULKTAG_DATASOURCE,
												DNET_PROVENANCE_ACTIONS)));
					if (czenodo.contains(c))
						dataInfoList
							.add(
								OafMapperUtils
									.dataInfo(
										TAGGING_TRUST, BULKTAG_DATA_INFO_TYPE, true,
										OafMapperUtils
											.qualifier(
												CLASS_ID_CZENODO, CLASS_NAME_BULKTAG_ZENODO, DNET_PROVENANCE_ACTIONS)));
					if (aconstraints.contains(c))
						dataInfoList
							.add(
								OafMapperUtils
									.dataInfo(
										TAGGING_TRUST, BULKTAG_DATA_INFO_TYPE, true,
										OafMapperUtils
											.qualifier(
												CLASS_ID_ADVANCED_CONSTRAINT, CLASS_NAME_BULKTAG_ADVANCED_CONSTRAINT,
												DNET_PROVENANCE_ACTIONS)));

					context.setDataInfo(dataInfoList);
					return context;
				})
			.collect(Collectors.toList());

		result.getContext().addAll(toaddcontext);
		return result;
	}

}
