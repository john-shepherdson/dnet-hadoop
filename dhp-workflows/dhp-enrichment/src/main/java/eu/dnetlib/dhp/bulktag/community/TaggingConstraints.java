
package eu.dnetlib.dhp.bulktag.community;

import java.io.Serializable;
import java.util.List;

public class TaggingConstraints implements Serializable {
	private List<TaggingConstraint> tags;

	public List<TaggingConstraint> getTags() {
		return tags;
	}

	public void setTags(List<TaggingConstraint> tags) {
		this.tags = tags;
	}
}
