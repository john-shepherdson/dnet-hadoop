
package eu.dnetlib.dhp.bulktag.community;

import java.io.Serializable;
import java.util.List;

public class TaggingConstraints implements Serializable {
	private List<TaggingConstraint> tags;
	private String graphPath;

	public List<TaggingConstraint> getTags() {
		return tags;
	}

	public void setTags(List<TaggingConstraint> tags) {
		this.tags = tags;
	}

	public String getGraphPath() {
		return graphPath;
	}

	public void setGraphPath(String graphPath) {
		this.graphPath = graphPath;
	}
}
