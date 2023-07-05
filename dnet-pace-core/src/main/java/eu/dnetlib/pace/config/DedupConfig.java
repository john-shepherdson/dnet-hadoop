package eu.dnetlib.pace.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import eu.dnetlib.pace.model.ClusteringDef;
import eu.dnetlib.pace.model.FieldDef;
import eu.dnetlib.pace.util.PaceException;
import org.antlr.stringtemplate.StringTemplate;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import eu.dnetlib.pace.tree.support.TreeNodeDef;


public class DedupConfig implements Config, Serializable {

	private static final Log log = LogFactory.getLog(DedupConfig.class);

	private static String CONFIG_TEMPLATE = "dedupConfig.st";

	private PaceConfig pace;

	private WfConfig wf;

	@JsonIgnore
	private Map<String, List<Pattern>> blacklists;

	private static Map<String, String> defaults = Maps.newHashMap();

	static {
		defaults.put("dedupRun", "001");
		defaults.put("entityType", "result");
		defaults.put("subEntityType", "resulttype");
		defaults.put("subEntityValue", "publication");
		defaults.put("orderField", "title");
		defaults.put("queueMaxSize", "2000");
		defaults.put("groupMaxSize", "10");
		defaults.put("slidingWindowSize", "200");
		defaults.put("rootBuilder", "result");
		defaults.put("includeChildren", "true");
		defaults.put("maxIterations", "20");
		defaults.put("idPath", "$.id");
	}

	public DedupConfig() {}

	public static DedupConfig load(final String json) {

		final DedupConfig config;
		try {
			config = new ObjectMapper().readValue(json, DedupConfig.class);
			config.getPace().initModel();
			config.getPace().initTranslationMap();

			config.blacklists = config.getPace().getBlacklists().entrySet()
					.stream()
					.collect(Collectors.toMap(e -> e.getKey(),
							e ->e.getValue().stream().filter(s -> !StringUtils.isBlank(s)).map(Pattern::compile).collect(Collectors.toList()) ));

			return config;
		} catch (IOException e) {
			throw new PaceException("Error in parsing configuration json", e);
		}

	}

	public static DedupConfig loadDefault() throws IOException {
		return loadDefault(new HashMap<String, String>());
	}

	public static DedupConfig loadDefault(final Map<String, String> params) throws IOException {

		final StringTemplate template = new StringTemplate(new DedupConfig().readFromClasspath(CONFIG_TEMPLATE));

		for (final Entry<String, String> e : defaults.entrySet()) {
			template.setAttribute(e.getKey(), e.getValue());
		}
		for (final Entry<String, String> e : params.entrySet()) {
			if (template.getAttribute(e.getKey()) != null) {
				template.getAttributes().computeIfPresent(e.getKey(), (o, o2) -> e.getValue());
			} else {
				template.setAttribute(e.getKey(), e.getValue());
			}
		}

		final String json = template.toString();
		return load(json);
	}

	private String readFromClasspath(final String resource) throws IOException {
		return IOUtils.toString(getClass().getResource(resource), StandardCharsets.UTF_8);
	}

	public PaceConfig getPace() {
		return pace;
	}

	public void setPace(final PaceConfig pace) {
		this.pace = pace;
	}

	public WfConfig getWf() {
		return wf;
	}

	public void setWf(final WfConfig wf) {
		this.wf = wf;
	}

	@Override
	public String toString() {
		try {
			return new ObjectMapper().writeValueAsString(this);
		} catch (IOException e) {
			throw new PaceException("unable to serialise configuration", e);
		}
	}

	@Override
	public Map<String, TreeNodeDef> decisionTree(){
		return getPace().getDecisionTree();
	}

	@Override
	public List<FieldDef> model() {
		return getPace().getModel();
	}

	@Override
	public Map<String, FieldDef> modelMap() {
		return getPace().getModelMap();
	}

	@Override
	public List<ClusteringDef> clusterings() {
		return getPace().getClustering();
	}

	@Override
	public Map<String, List<Pattern>> blacklists() {
		return blacklists;
	}

	@Override
	public Map<String, String> translationMap() {
		return getPace().translationMap();
	}

}
