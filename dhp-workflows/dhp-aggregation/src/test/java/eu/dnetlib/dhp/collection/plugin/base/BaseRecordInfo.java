
package eu.dnetlib.dhp.collection.plugin.base;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BaseRecordInfo implements Serializable {

	private static final long serialVersionUID = -8848232018350074593L;

	private String id;
	private List<BaseCollectionInfo> collections = new ArrayList<>();
	private List<String> paths = new ArrayList<>();
	private List<String> types = new ArrayList<>();

	public String getId() {
		return this.id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public List<String> getPaths() {
		return this.paths;
	}

	public void setPaths(final List<String> paths) {
		this.paths = paths;
	}

	public List<String> getTypes() {
		return this.types;
	}

	public void setTypes(final List<String> types) {
		this.types = types;
	}

	public List<BaseCollectionInfo> getCollections() {
		return this.collections;
	}

	public void setCollections(final List<BaseCollectionInfo> collections) {
		this.collections = collections;
	}

}
