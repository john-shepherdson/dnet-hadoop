
package eu.dnetlib.dhp.oa.dedup.model;

import java.io.Serializable;

public class ParentChildRel implements Serializable {

	String source_original_id;
	String target_original_id;
	String rel_type;

	public ParentChildRel() {
	}

	public ParentChildRel(String source_original_id, String target_original_id, String rel_type) {
		this.source_original_id = source_original_id;
		this.target_original_id = target_original_id;
		this.rel_type = rel_type;
	}

	public String getSource_original_id() {
		return source_original_id;
	}

	public void setSource_original_id(String source_original_id) {
		this.source_original_id = source_original_id;
	}

	public String getTarget_original_id() {
		return target_original_id;
	}

	public void setTarget_original_id(String target_original_id) {
		this.target_original_id = target_original_id;
	}

	public String getRel_type() {
		return rel_type;
	}

	public void setRel_type(String rel_type) {
		this.rel_type = rel_type;
	}

	@Override
	public String toString() {
		return "ParentChildRel{" +
			"source_original_id='" + source_original_id + '\'' +
			", target_original_id='" + target_original_id + '\'' +
			", rel_type='" + rel_type + '\'' +
			'}';
	}
}
