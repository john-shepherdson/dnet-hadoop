
package eu.dnetlib.dhp.tag.bean;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class TaggingConstraint  implements Serializable {


	private String id;
	private Map<String, String> inputs;
	private List<Pair> selects ;
	private String entityToTag;
	private List<Constraints> criteria;
	private String resultTable;

	public String getResultTable() {
		return resultTable;
	}

	public void setResultTable(String resultTable) {
		this.resultTable = resultTable;
	}

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

	public List<Pair> getSelects() {
		return selects;
	}

	public void setSelects(List<Pair> selects) {
		this.selects = selects;
	}

	public List<Constraints> getCriteria() {
		return criteria;
	}

	public void setCriteria(List<Constraints> criteria) {
		this.criteria = criteria;
	}
}
