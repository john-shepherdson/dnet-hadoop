
package eu.dnetlib.dhp.bulktag.community;

import static eu.dnetlib.dhp.bulktag.community.TaggingConstants.*;
import static eu.dnetlib.dhp.schema.common.ModelConstants.*;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;


import eu.dnetlib.dhp.bulktag.Tagging;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import eu.dnetlib.dhp.bulktag.actions.MapModel;
import eu.dnetlib.dhp.bulktag.actions.Parameters;
import eu.dnetlib.dhp.bulktag.eosc.EoscIFTag;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import scala.Tuple2;

/** Created by miriam on 02/08/2018. */
public class ResultTagger implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(ResultTagger.class);

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

	private Map<String, List<String>> getParamMap(final Result result, Map<String, MapModel> params)
		throws NoSuchMethodException, InvocationTargetException {
		Map<String, List<String>> param = new HashMap<>();
		String json = new Gson().toJson(result, Result.class);
		DocumentContext jsonContext = JsonPath.parse(json);

		if (params == null) {
			params = new HashMap<>();
		}
		for (String key : params.keySet()) {
			MapModel mapModel = params.get(key);

			try {
				String path = mapModel.getPath();
				Object obj = jsonContext.read(path);
				List<String> pathValue;
				if (obj instanceof java.lang.String)
					pathValue = Arrays.asList((String) obj);
				else
					pathValue = (List<String>) obj;
				if (Optional.ofNullable(mapModel.getAction()).isPresent()) {
					Class<?> c = Class.forName(mapModel.getAction().getClazz());
					Object class_instance = c.newInstance();
					Method setField = c.getMethod("setValue", String.class);
					setField.invoke(class_instance, pathValue.get(0));
					for (Parameters p : mapModel.getAction().getParams()) {
						setField = c.getMethod("set" + p.getParamName(), String.class);
						setField.invoke(class_instance, p.getParamValue());
					}

					param
						.put(
							key, Arrays
								.asList((String) c.getMethod(mapModel.getAction().getMethod()).invoke(class_instance)));

				}

				else {
					param.put(key, pathValue);
				}

			} catch (PathNotFoundException | ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
				param.put(key, new ArrayList<>());
			}
		}
		return param;

	}

	public <R extends Result> Tagging enrichContextCriteria(
		final R result, final CommunityConfiguration conf, final Map<String, MapModel> criteria, SelectionConstraints taggingConstraints)
		throws InvocationTargetException, NoSuchMethodException {

		// Verify if the entity is deletedbyinference. In case verify if to clean the context list
		// from all the zenodo communities
		if (result.getDataInfo().getDeletedbyinference()) {
			clearContext(result);
			return Tagging.newInstance(result, null);
		}

		String retString = null;
		final Map<String, List<String>> param = getParamMap(result, criteria);

		// Execute the EOSCTag for the services
		switch (result.getResulttype().getClassid()) {
			case PUBLICATION_RESULTTYPE_CLASSID:
				break;
			case SOFTWARE_RESULTTYPE_CLASSID:
				EoscIFTag.tagForSoftware(result);
				break;
			case DATASET_RESULTTYPE_CLASSID:
				EoscIFTag.tagForDataset(result);
				break;
			case ORP_RESULTTYPE_CLASSID:
				EoscIFTag.tagForOther(result);
				break;
		}

//adding code for tagging of results searching supplementaryMaterial
		if(taggingConstraints.getCriteria().stream().anyMatch(crit -> crit.verifyCriteria(param)))
			retString = "supplementary";

		// communities contains all the communities to be not added to the context
		final Set<String> removeCommunities = new HashSet<>();

		// if (conf.getRemoveConstraintsMap().keySet().size() > 0)
		conf
			.getRemoveConstraintsMap()
			.keySet()
			.forEach(
				communityId -> {
					// log.info("Remove constraints for " + communityId);
					if (conf.getRemoveConstraintsMap().keySet().contains(communityId) &&
						conf.getRemoveConstraintsMap().get(communityId).getCriteria() != null &&
						conf
							.getRemoveConstraintsMap()
							.get(communityId)
							.getCriteria()
							.stream()
							.anyMatch(crit -> crit.verifyCriteria(param)))
						removeCommunities.add(communityId);
				});

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
		final Set<String> collfrom = new HashSet<>();
		final Set<String> hostdby = new HashSet<>();

		if (Objects.nonNull(result.getInstance())) {
			for (Instance i : result.getInstance()) {
				if (Objects.nonNull(i.getCollectedfrom()) && Objects.nonNull(i.getCollectedfrom().getKey())) {
					collfrom.add(i.getCollectedfrom().getKey());
				}
				if (Objects.nonNull(i.getHostedby()) && Objects.nonNull(i.getHostedby().getKey())) {
					hostdby.add(i.getHostedby().getKey());
				}

			}

			collfrom
				.forEach(
					dsId -> datasources
						.addAll(
							conf.getCommunityForDatasource(dsId, param)));
			hostdby.forEach(dsId -> {
				datasources
					.addAll(
						conf.getCommunityForDatasource(dsId, param));
				if (conf.isEoscDatasource(dsId)) {
					datasources.add("eosc");
				}

			});
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
				if (!removeCommunities.contains(communityId) &&
					conf.getSelectionConstraintsMap().get(communityId).getCriteria() != null &&
					conf
						.getSelectionConstraintsMap()
						.get(communityId)
						.getCriteria()
						.stream()
						.anyMatch(crit -> crit.verifyCriteria(param)))
					aconstraints.add(communityId);
			});

		communities.addAll(aconstraints);

		communities.removeAll(removeCommunities);

		if (aconstraints.size() > 0)
			log.info("Found {} for advancedConstraints ", aconstraints.size());

		clearContext(result);

		/* Verify if there is something to bulktag */
		if (communities.isEmpty()) {
			return Tagging.newInstance(result, retString);
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
									false, BULKTAG_DATA_INFO_TYPE, true, false,
									OafMapperUtils
										.qualifier(
											CLASS_ID_SUBJECT, CLASS_NAME_BULKTAG_SUBJECT, DNET_PROVENANCE_ACTIONS,
											DNET_PROVENANCE_ACTIONS),
									TAGGING_TRUST));
				if (datasources.contains(cId))
					dataInfoList
						.add(
							OafMapperUtils
								.dataInfo(
									false, BULKTAG_DATA_INFO_TYPE, true, false,
									OafMapperUtils
										.qualifier(
											CLASS_ID_DATASOURCE, CLASS_NAME_BULKTAG_DATASOURCE, DNET_PROVENANCE_ACTIONS,
											DNET_PROVENANCE_ACTIONS),
									TAGGING_TRUST));
				if (czenodo.contains(cId))
					dataInfoList
						.add(
							OafMapperUtils
								.dataInfo(
									false, BULKTAG_DATA_INFO_TYPE, true, false,
									OafMapperUtils
										.qualifier(
											CLASS_ID_CZENODO, CLASS_NAME_BULKTAG_ZENODO, DNET_PROVENANCE_ACTIONS,
											DNET_PROVENANCE_ACTIONS),
									TAGGING_TRUST));
				if (aconstraints.contains(cId))
					dataInfoList
						.add(
							OafMapperUtils
								.dataInfo(
									false, BULKTAG_DATA_INFO_TYPE, true, false,
									OafMapperUtils
										.qualifier(
											CLASS_ID_ADVANCED_CONSTRAINT, CLASS_NAME_BULKTAG_ADVANCED_CONSTRAINT,
											DNET_PROVENANCE_ACTIONS, DNET_PROVENANCE_ACTIONS),
									TAGGING_TRUST));

			}
		});

		communities
			.removeAll(
				result.getContext().stream().map(Context::getId).collect(Collectors.toSet()));

		if (communities.isEmpty())
			return Tagging.newInstance(result, retString);

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
										false, BULKTAG_DATA_INFO_TYPE, true, false,
										OafMapperUtils
											.qualifier(
												CLASS_ID_SUBJECT, CLASS_NAME_BULKTAG_SUBJECT, DNET_PROVENANCE_ACTIONS,
												DNET_PROVENANCE_ACTIONS),
										TAGGING_TRUST));
					if (datasources.contains(c))
						dataInfoList
							.add(
								OafMapperUtils
									.dataInfo(
										false, BULKTAG_DATA_INFO_TYPE, true, false,
										OafMapperUtils
											.qualifier(
												CLASS_ID_DATASOURCE, CLASS_NAME_BULKTAG_DATASOURCE,
												DNET_PROVENANCE_ACTIONS, DNET_PROVENANCE_ACTIONS),
										TAGGING_TRUST));
					if (czenodo.contains(c))
						dataInfoList
							.add(
								OafMapperUtils
									.dataInfo(
										false, BULKTAG_DATA_INFO_TYPE, true, false,
										OafMapperUtils
											.qualifier(
												CLASS_ID_CZENODO, CLASS_NAME_BULKTAG_ZENODO, DNET_PROVENANCE_ACTIONS,
												DNET_PROVENANCE_ACTIONS),
										TAGGING_TRUST));
					if (aconstraints.contains(c))
						dataInfoList
							.add(
								OafMapperUtils
									.dataInfo(
										false, BULKTAG_DATA_INFO_TYPE, true, false,
										OafMapperUtils
											.qualifier(
												CLASS_ID_ADVANCED_CONSTRAINT, CLASS_NAME_BULKTAG_ADVANCED_CONSTRAINT,
												DNET_PROVENANCE_ACTIONS, DNET_PROVENANCE_ACTIONS),
										TAGGING_TRUST));

					context.setDataInfo(dataInfoList);
					return context;
				})
			.collect(Collectors.toList());

		result.getContext().addAll(toaddcontext);
		return Tagging.newInstance(result, retString);
	}

}
