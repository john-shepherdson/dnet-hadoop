package eu.dnetlib.dhp.community;

import static eu.dnetlib.dhp.community.TagginConstants.*;

import com.google.gson.Gson;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.schema.oaf.*;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

/** Created by miriam on 02/08/2018. */
public class ResultTagger implements Serializable {

    private String trust = "0.8";

    private boolean clearContext(Result result) {
        int tmp = result.getContext().size();
        List<Context> clist =
                result.getContext().stream()
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
                // throw e;
            }
        }
        return param;
    }

    public <R extends Result> R enrichContextCriteria(
            final R result, final CommunityConfiguration conf, final Map<String, String> criteria) {

        //    }
        //    public Result enrichContextCriteria(final Result result, final CommunityConfiguration
        // conf, final Map<String,String> criteria) {
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
        result.getSubject().stream()
                .map(subject -> subject.getValue())
                .filter(StringUtils::isNotBlank)
                .map(String::toLowerCase)
                .map(String::trim)
                .collect(Collectors.toCollection(HashSet::new))
                .forEach(s -> subjects.addAll(conf.getCommunityForSubjectValue(s)));

        communities.addAll(subjects);

        // Tagging for datasource
        final Set<String> datasources = new HashSet<>();
        final Set<String> tmp = new HashSet<>();

        for (Instance i : result.getInstance()) {
            tmp.add(StringUtils.substringAfter(i.getCollectedfrom().getKey(), "|"));
            tmp.add(StringUtils.substringAfter(i.getHostedby().getKey(), "|"));
        }

        result.getInstance().stream()
                .map(i -> new Pair<>(i.getCollectedfrom().getKey(), i.getHostedby().getKey()))
                .flatMap(p -> Stream.of(p.getFst(), p.getSnd()))
                .map(s -> StringUtils.substringAfter(s, "|"))
                .collect(Collectors.toCollection(HashSet::new))
                .forEach(dsId -> datasources.addAll(conf.getCommunityForDatasource(dsId, param)));

        communities.addAll(datasources);

        /*Tagging for Zenodo Communities*/
        final Set<String> czenodo = new HashSet<>();
        result.getContext().stream()
                .filter(c -> c.getId().contains(ZENODO_COMMUNITY_INDICATOR))
                .collect(Collectors.toList())
                .forEach(
                        c ->
                                czenodo.addAll(
                                        conf.getCommunityForZenodoCommunityValue(
                                                c.getId()
                                                        .substring(c.getId().lastIndexOf("/") + 1)
                                                        .trim())));

        communities.addAll(czenodo);

        clearContext(result);

        /*Verify if there is something to bulktag*/
        if (communities.isEmpty()) {
            return result;
        }

        result.getContext().stream()
                .map(
                        c -> {
                            if (communities.contains(c.getId())) {
                                Optional<List<DataInfo>> opt_dataInfoList =
                                        Optional.ofNullable(c.getDataInfo());
                                List<DataInfo> dataInfoList;
                                if (opt_dataInfoList.isPresent())
                                    dataInfoList = opt_dataInfoList.get();
                                else {
                                    dataInfoList = new ArrayList<>();
                                    c.setDataInfo(dataInfoList);
                                }
                                if (subjects.contains(c.getId()))
                                    dataInfoList.add(
                                            getDataInfo(
                                                    BULKTAG_DATA_INFO_TYPE,
                                                    CLASS_ID_SUBJECT,
                                                    CLASS_NAME_BULKTAG_SUBJECT));
                                if (datasources.contains(c.getId()))
                                    dataInfoList.add(
                                            getDataInfo(
                                                    BULKTAG_DATA_INFO_TYPE,
                                                    CLASS_ID_DATASOURCE,
                                                    CLASS_NAME_BULKTAG_DATASOURCE));
                                if (czenodo.contains(c.getId()))
                                    dataInfoList.add(
                                            getDataInfo(
                                                    BULKTAG_DATA_INFO_TYPE,
                                                    CLASS_ID_CZENODO,
                                                    CLASS_NAME_BULKTAG_ZENODO));
                            }
                            return c;
                        })
                .collect(Collectors.toList());

        communities.removeAll(
                result.getContext().stream().map(c -> c.getId()).collect(Collectors.toSet()));

        if (communities.isEmpty()) return result;

        List<Context> toaddcontext =
                communities.stream()
                        .map(
                                c -> {
                                    Context context = new Context();
                                    context.setId(c);
                                    List<DataInfo> dataInfoList = new ArrayList<>();
                                    if (subjects.contains(c))
                                        dataInfoList.add(
                                                getDataInfo(
                                                        BULKTAG_DATA_INFO_TYPE,
                                                        CLASS_ID_SUBJECT,
                                                        CLASS_NAME_BULKTAG_SUBJECT));
                                    if (datasources.contains(c))
                                        dataInfoList.add(
                                                getDataInfo(
                                                        BULKTAG_DATA_INFO_TYPE,
                                                        CLASS_ID_DATASOURCE,
                                                        CLASS_NAME_BULKTAG_DATASOURCE));
                                    if (czenodo.contains(c))
                                        dataInfoList.add(
                                                getDataInfo(
                                                        BULKTAG_DATA_INFO_TYPE,
                                                        CLASS_ID_CZENODO,
                                                        CLASS_NAME_BULKTAG_ZENODO));
                                    context.setDataInfo(dataInfoList);
                                    return context;
                                })
                        .collect(Collectors.toList());

        result.getContext().addAll(toaddcontext);
        return result;
    }

    public static DataInfo getDataInfo(
            String inference_provenance, String inference_class_id, String inference_class_name) {
        DataInfo di = new DataInfo();
        di.setInferred(true);
        di.setInferenceprovenance(inference_provenance);
        di.setProvenanceaction(getQualifier(inference_class_id, inference_class_name));
        return di;
    }

    public static Qualifier getQualifier(String inference_class_id, String inference_class_name) {
        Qualifier pa = new Qualifier();
        pa.setClassid(inference_class_id);
        pa.setClassname(inference_class_name);
        pa.setSchemeid(DNET_SCHEMA_ID);
        pa.setSchemename(DNET_SCHEMA_NAME);
        return pa;
    }
}
