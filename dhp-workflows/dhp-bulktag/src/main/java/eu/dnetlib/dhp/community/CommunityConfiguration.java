package eu.dnetlib.dhp.community;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.dnetlib.dhp.selectioncriteria.InterfaceAdapter;
import eu.dnetlib.dhp.selectioncriteria.Selection;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Created by miriam on 02/08/2018. */
public class CommunityConfiguration implements Serializable {

    private static final Log log = LogFactory.getLog(CommunityConfiguration.class);

    private Map<String, Community> communities;

    // map subject -> communityid
    private Map<String, List<Pair<String, SelectionConstraints>>> subjectMap = new HashMap<>();
    // map datasourceid -> communityid
    private Map<String, List<Pair<String, SelectionConstraints>>> datasourceMap = new HashMap<>();
    // map zenodocommunityid -> communityid
    private Map<String, List<Pair<String, SelectionConstraints>>> zenodocommunityMap =
            new HashMap<>();

    public Map<String, List<Pair<String, SelectionConstraints>>> getSubjectMap() {
        return subjectMap;
    }

    public void setSubjectMap(Map<String, List<Pair<String, SelectionConstraints>>> subjectMap) {
        this.subjectMap = subjectMap;
    }

    public Map<String, List<Pair<String, SelectionConstraints>>> getDatasourceMap() {
        return datasourceMap;
    }

    public void setDatasourceMap(
            Map<String, List<Pair<String, SelectionConstraints>>> datasourceMap) {
        this.datasourceMap = datasourceMap;
    }

    public Map<String, List<Pair<String, SelectionConstraints>>> getZenodocommunityMap() {
        return zenodocommunityMap;
    }

    public void setZenodocommunityMap(
            Map<String, List<Pair<String, SelectionConstraints>>> zenodocommunityMap) {
        this.zenodocommunityMap = zenodocommunityMap;
    }

    CommunityConfiguration(final Map<String, Community> communities) {
        this.communities = communities;
        init();
    }

    void init() {

        if (subjectMap == null) {
            subjectMap = Maps.newHashMap();
        }
        if (datasourceMap == null) {
            datasourceMap = Maps.newHashMap();
        }
        if (zenodocommunityMap == null) {
            zenodocommunityMap = Maps.newHashMap();
        }

        for (Community c : getCommunities().values()) {
            // get subjects
            final String id = c.getId();
            for (String sbj : c.getSubjects()) {
                Pair<String, SelectionConstraints> p = new Pair<>(id, new SelectionConstraints());
                add(sbj.toLowerCase().trim(), p, subjectMap);
            }
            // get datasources
            for (Datasource d : c.getDatasources()) {

                add(d.getOpenaireId(), new Pair<>(id, d.getSelectionConstraints()), datasourceMap);
            }
            // get zenodo communities
            for (ZenodoCommunity zc : c.getZenodoCommunities()) {
                add(
                        zc.getZenodoCommunityId(),
                        new Pair<>(id, zc.getSelCriteria()),
                        zenodocommunityMap);
            }
        }
    }

    private void add(
            String key,
            Pair<String, SelectionConstraints> value,
            Map<String, List<Pair<String, SelectionConstraints>>> map) {
        List<Pair<String, SelectionConstraints>> values = map.get(key);

        if (values == null) {
            values = new ArrayList<>();
            map.put(key, values);
        }
        values.add(value);
    }

    public List<Pair<String, SelectionConstraints>> getCommunityForSubject(String sbj) {
        return subjectMap.get(sbj);
    }

    public List<Pair<String, SelectionConstraints>> getCommunityForDatasource(String dts) {
        return datasourceMap.get(dts);
    }

    public List<String> getCommunityForDatasource(
            final String dts, final Map<String, List<String>> param) {
        List<Pair<String, SelectionConstraints>> lp = datasourceMap.get(dts);
        if (lp == null) return Lists.newArrayList();

        return lp.stream()
                .map(
                        p -> {
                            if (p.getSnd() == null) return p.getFst();
                            if (((SelectionConstraints) p.getSnd()).verifyCriteria(param))
                                return p.getFst();
                            else return null;
                        })
                .filter(st -> (st != null))
                .collect(Collectors.toList());
    }

    public List<Pair<String, SelectionConstraints>> getCommunityForZenodoCommunity(String zc) {
        return zenodocommunityMap.get(zc);
    }

    public List<String> getCommunityForSubjectValue(String value) {

        return getContextIds(subjectMap.get(value));
    }

    public List<String> getCommunityForDatasourceValue(String value) {

        return getContextIds(datasourceMap.get(value.toLowerCase()));
    }

    public List<String> getCommunityForZenodoCommunityValue(String value) {

        return getContextIds(zenodocommunityMap.get(value.toLowerCase()));
    }

    private List<String> getContextIds(List<Pair<String, SelectionConstraints>> list) {
        if (list != null) {
            return list.stream().map(p -> p.getFst()).collect(Collectors.toList());
        }
        return Lists.newArrayList();
    }

    public Map<String, Community> getCommunities() {
        return communities;
    }

    public void setCommunities(Map<String, Community> communities) {
        this.communities = communities;
    }

    public String toJson() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(Selection.class, new InterfaceAdapter());
        Gson gson = builder.create();

        return gson.toJson(this);
    }

    public int size() {
        return communities.keySet().size();
    }

    public Community getCommunityById(String id) {
        return communities.get(id);
    }

    public List<Community> getCommunityList() {
        return Lists.newLinkedList(communities.values());
    }
}
