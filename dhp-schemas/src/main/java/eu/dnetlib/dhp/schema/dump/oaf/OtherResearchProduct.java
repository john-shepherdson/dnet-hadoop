
package eu.dnetlib.dhp.schema.dump.oaf;

import eu.dnetlib.dhp.schema.common.ModelConstants;


import java.io.Serializable;
import java.util.List;

public class OtherResearchProduct extends Result implements Serializable {

	private List<String> contactperson;

	private List<String> contactgroup;

	private List<String> tool;

	public OtherResearchProduct() {
		setResulttype(ModelConstants.ORP_DEFAULT_RESULTTYPE.getClassname());
	}

	public List<String> getContactperson() {
		return contactperson;
	}

	public void setContactperson(List<String> contactperson) {
		this.contactperson = contactperson;
	}

	public List<String> getContactgroup() {
		return contactgroup;
	}

	public void setContactgroup(List<String> contactgroup) {
		this.contactgroup = contactgroup;
	}

	public List<String> getTool() {
		return tool;
	}

	public void setTool(List<String> tool) {
		this.tool = tool;
	}
}
