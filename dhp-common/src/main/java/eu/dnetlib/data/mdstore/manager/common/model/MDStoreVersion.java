
package eu.dnetlib.data.mdstore.manager.common.model;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
@Table(name = "mdstore_versions")
public class MDStoreVersion implements Serializable {

	/** */
	private static final long serialVersionUID = -4763494442274298339L;

	@Id
	@Column(name = "id")
	private String id;

	@Column(name = "mdstore")
	private String mdstore;

	@Column(name = "writing")
	private boolean writing;

	@Column(name = "readcount")
	private int readCount = 0;

	@Column(name = "lastupdate")
	@Temporal(TemporalType.TIMESTAMP)
	private Date lastUpdate;

	@Column(name = "size")
	private long size = 0;

	@Column(name = "hdfs_path")
	private String hdfsPath;

	public static MDStoreVersion newInstance(final String mdId, final boolean writing, final String hdfsBasePath) {
		final MDStoreVersion v = new MDStoreVersion();

		final String versionId = mdId + "-" + new Date().getTime();
		v.setId(versionId);
		v.setMdstore(mdId);
		v.setLastUpdate(null);
		v.setWriting(writing);
		v.setReadCount(0);
		v.setSize(0);
		v.setHdfsPath(String.format("%s/%s/%s", hdfsBasePath, mdId, versionId));

		return v;
	}

	public String getId() {
		return id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getMdstore() {
		return mdstore;
	}

	public void setMdstore(final String mdstore) {
		this.mdstore = mdstore;
	}

	public boolean isWriting() {
		return writing;
	}

	public void setWriting(final boolean writing) {
		this.writing = writing;
	}

	public int getReadCount() {
		return readCount;
	}

	public void setReadCount(final int readCount) {
		this.readCount = readCount;
	}

	public Date getLastUpdate() {
		return lastUpdate;
	}

	public void setLastUpdate(final Date lastUpdate) {
		this.lastUpdate = lastUpdate;
	}

	public long getSize() {
		return size;
	}

	public void setSize(final long size) {
		this.size = size;
	}

	public String getHdfsPath() {
		return hdfsPath;
	}

	public void setHdfsPath(final String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}

	@Override
	public String toString() {
		return String
			.format(
				"MDStoreVersion [id=%s, mdstore=%s, writing=%s, readCount=%s, lastUpdate=%s, size=%s, hdfsPath=%s]", id,
				mdstore, writing, readCount, lastUpdate, size, hdfsPath);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof MDStoreVersion)) {
			return false;
		}
		final MDStoreVersion other = (MDStoreVersion) obj;
		return Objects.equals(id, other.id);
	}
}
