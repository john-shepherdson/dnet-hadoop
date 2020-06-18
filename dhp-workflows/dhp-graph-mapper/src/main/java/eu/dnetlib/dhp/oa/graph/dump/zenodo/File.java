
package eu.dnetlib.dhp.oa.graph.dump.zenodo;

import java.io.Serializable;

import net.minidev.json.annotate.JsonIgnore;

public class File implements Serializable {
	private String checksum;
	private String filename;
	private long filesize;
	private String id;

	@JsonIgnore
	// private Links links;

	public String getChecksum() {
		return checksum;
	}

	public void setChecksum(String checksum) {
		this.checksum = checksum;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public long getFilesize() {
		return filesize;
	}

	public void setFilesize(long filesize) {
		this.filesize = filesize;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

//    @JsonIgnore
//    public Links getLinks() {
//        return links;
//    }
//
//    @JsonIgnore
//    public void setLinks(Links links) {
//        this.links = links;
//    }
}
