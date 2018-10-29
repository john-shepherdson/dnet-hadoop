package eu.dnetlib.pace.config;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import eu.dnetlib.pace.condition.ConditionAlgo;
import eu.dnetlib.pace.model.ClusteringDef;
import eu.dnetlib.pace.model.CondDef;
import eu.dnetlib.pace.model.FieldDef;
import eu.dnetlib.pace.util.PaceResolver;
import org.apache.commons.collections.CollectionUtils;
import org.codehaus.jackson.annotate.JsonIgnore;

public class PaceConfig implements Serializable {

	private List<FieldDef> model;
	private List<CondDef> strictConditions;
	private List<CondDef> conditions;
	private List<ClusteringDef> clustering;
	private Map<String, List<String>> blacklists;

	private Map<String, FieldDef> modelMap;

	public static PaceResolver paceResolver;

	public PaceConfig() {}

	public void initModel() {
		modelMap = Maps.newHashMap();
		for(FieldDef fd : getModel()) {
			modelMap.put(fd.getName(), fd);
		}

		paceResolver = new PaceResolver();
	}

	public List<FieldDef> getModel() {
		return model;
	}

	public void setModel(final List<FieldDef> model) {
		this.model = model;
	}

	public List<CondDef> getStrictConditions() {
		return strictConditions;
	}

	public void setStrictConditions(final List<CondDef> strictConditions) {
		this.strictConditions = strictConditions;
	}

	public List<CondDef> getConditions() {
		return conditions;
	}

	@JsonIgnore
	public List<ConditionAlgo> getConditionAlgos() {
		return asConditionAlgos(getConditions());
	}

	@JsonIgnore
	public List<ConditionAlgo> getStrictConditionAlgos() {
		return asConditionAlgos(getStrictConditions());
	}

	public void setConditions(final List<CondDef> conditions) {
		this.conditions = conditions;
	}

	public List<ClusteringDef> getClustering() {
		return clustering;
	}

	public void setClustering(final List<ClusteringDef> clustering) {
		this.clustering = clustering;
	}

	public Map<String, List<String>> getBlacklists() {
		return blacklists;
	}

	public void setBlacklists(final Map<String, List<String>> blacklists) {
		this.blacklists = blacklists;
	}

	public Map<String, FieldDef> getModelMap() {
		return modelMap;
	}

	public void setModelMap(final Map<String, FieldDef> modelMap) {
		this.modelMap = modelMap;
	}

	// helper

	private List<ConditionAlgo> asConditionAlgos(final List<CondDef> defs) {
		final List<ConditionAlgo> algos = Lists.newArrayList();
		if (CollectionUtils.isEmpty(defs)) return algos;
		for (final CondDef cd : defs) {
			final List<FieldDef> fields = getModel().stream()
					.filter(fd -> cd.getFields().contains(fd.getName()))
					.collect(Collectors.toList());
			algos.add(cd.conditionAlgo(fields));
		}
		return algos;
	}

}
