
package eu.dnetlib.dhp.tag.bean;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class TaggingConstraint  implements Serializable {


	private String id;
			private Map<String, String> inputs;
			private List<String> selects ;
	private String entityToTag;
	private List<Constraints> criteria;

	public String getEntityToTag() {
		return entityToTag;
	}

	public void setEntityToTag(String entityToTag) {
		this.entityToTag = entityToTag;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<String, String> getInputs() {
		return inputs;
	}

	public void setInputs(Map<String, String> inputs) {
		this.inputs = inputs;
	}

	public List<String> getSelects() {
		return selects;
	}

	public void setSelects(List<String> selects) {
		this.selects = selects;
	}

	public List<Constraints> getCriteria() {
		return criteria;
	}

	public void setCriteria(List<Constraints> criteria) {
		this.criteria = criteria;
	}
}
