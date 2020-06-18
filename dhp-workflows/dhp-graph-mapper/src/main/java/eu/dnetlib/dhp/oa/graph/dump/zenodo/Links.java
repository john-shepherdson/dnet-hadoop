
package eu.dnetlib.dhp.oa.graph.dump.zenodo;

import java.io.Serializable;

public class Links implements Serializable {

	private String bucket;

	private String discard;

	private String edit;
	private String files;
	private String html;
	private String latest_draft;
	private String latest_draft_html;
	private String publish;

	private String self;

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public String getDiscard() {
		return discard;
	}

	public void setDiscard(String discard) {
		this.discard = discard;
	}

	public String getEdit() {
		return edit;
	}

	public void setEdit(String edit) {
		this.edit = edit;
	}

	public String getFiles() {
		return files;
	}

	public void setFiles(String files) {
		this.files = files;
	}

	public String getHtml() {
		return html;
	}

	public void setHtml(String html) {
		this.html = html;
	}

	public String getLatest_draft() {
		return latest_draft;
	}

	public void setLatest_draft(String latest_draft) {
		this.latest_draft = latest_draft;
	}

	public String getLatest_draft_html() {
		return latest_draft_html;
	}

	public void setLatest_draft_html(String latest_draft_html) {
		this.latest_draft_html = latest_draft_html;
	}

	public String getPublish() {
		return publish;
	}

	public void setPublish(String publish) {
		this.publish = publish;
	}

	public String getSelf() {
		return self;
	}

	public void setSelf(String self) {
		this.self = self;
	}
}
