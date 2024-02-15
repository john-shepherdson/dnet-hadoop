package eu.dnetlib.dhp.collection.plugin.base;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class BaseRecordInfo implements Serializable {

	private static final long serialVersionUID = -8848232018350074593L;

	private String id;
	private Map<String, Map<String, String>> collections = new HashMap<>();
	private Set<String> paths = new LinkedHashSet<>();
	private Set<String> types = new LinkedHashSet<>();

	public String getId() {
		return this.id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public Set<String> getPaths() {
		return this.paths;
	}

	public void setPaths(final Set<String> paths) {
		this.paths = paths;
	}

	public Set<String> getTypes() {
		return this.types;
	}

	public void setTypes(final Set<String> types) {
		this.types = types;
	}

	public Map<String, Map<String, String>> getCollections() {
		return this.collections;
	}

	public void setCollections(final Map<String, Map<String, String>> collections) {
		this.collections = collections;
	}

}
