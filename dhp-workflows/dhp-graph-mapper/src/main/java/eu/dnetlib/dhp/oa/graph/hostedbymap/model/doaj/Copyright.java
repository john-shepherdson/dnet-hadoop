
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;

public class Copyright implements Serializable {
	private Boolean author_retains;
	private String url;

	public Boolean getAuthor_retains() {
		return author_retains;
	}

	public void setAuthor_retains(Boolean author_retains) {
		this.author_retains = author_retains;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
}
