
package eu.dnetlib.dhp.oa.graph.dump.zenodo;

import java.io.Serializable;
import java.util.List;

public class ZenodoModel implements Serializable {

	private String conceptrecid;
	private String created;

	private List<File> files;
	private String id;
	private Links links;
	private Metadata metadata;
	private String modified;
	private String owner;
	private String record_id;
	private String state;
	private boolean submitted;
	private String title;

	public String getConceptrecid() {
		return conceptrecid;
	}

	public void setConceptrecid(String conceptrecid) {
		this.conceptrecid = conceptrecid;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public List<File> getFiles() {
		return files;
	}

	public void setFiles(List<File> files) {
		this.files = files;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Links getLinks() {
		return links;
	}

	public void setLinks(Links links) {
		this.links = links;
	}

	public Metadata getMetadata() {
		return metadata;
	}

	public void setMetadata(Metadata metadata) {
		this.metadata = metadata;
	}

	public String getModified() {
		return modified;
	}

	public void setModified(String modified) {
		this.modified = modified;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getRecord_id() {
		return record_id;
	}

	public void setRecord_id(String record_id) {
		this.record_id = record_id;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public boolean isSubmitted() {
		return submitted;
	}

	public void setSubmitted(boolean submitted) {
		this.submitted = submitted;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
}
