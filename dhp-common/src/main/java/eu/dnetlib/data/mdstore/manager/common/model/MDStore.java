
package eu.dnetlib.data.mdstore.manager.common.model;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
@Table(name = "mdstores")
public class MDStore implements Serializable {

	/** */
	private static final long serialVersionUID = 3160530489149700055L;

	@Id
	@Column(name = "id")
	private String id;

	@Column(name = "format")
	private String format;

	@Column(name = "layout")
	private String layout;

	@Column(name = "interpretation")
	private String interpretation;

	@Column(name = "datasource_name")
	private String datasourceName;

	@Column(name = "datasource_id")
	private String datasourceId;

	@Column(name = "api_id")
	private String apiId;

	@Column(name = "hdfs_path")
	private String hdfsPath;

	@Column(name = "creation_date")
	@Temporal(TemporalType.TIMESTAMP)
	private Date creationDate;

	public String getId() {
		return id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(final String format) {
		this.format = format;
	}

	public String getLayout() {
		return layout;
	}

	public void setLayout(final String layout) {
		this.layout = layout;
	}

	public String getInterpretation() {
		return interpretation;
	}

	public void setInterpretation(final String interpretation) {
		this.interpretation = interpretation;
	}

	public String getDatasourceName() {
		return datasourceName;
	}

	public void setDatasourceName(final String datasourceName) {
		this.datasourceName = datasourceName;
	}

	public String getDatasourceId() {
		return datasourceId;
	}

	public void setDatasourceId(final String datasourceId) {
		this.datasourceId = datasourceId;
	}

	public String getApiId() {
		return apiId;
	}

	public void setApiId(final String apiId) {
		this.apiId = apiId;
	}

	public String getHdfsPath() {
		return hdfsPath;
	}

	public void setHdfsPath(final String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(final Date creationDate) {
		this.creationDate = creationDate;
	}

	public static MDStore newInstance(
		final String format,
		final String layout,
		final String interpretation,
		final String hdfsBasePath) {
		return newInstance(format, layout, interpretation, null, null, null, hdfsBasePath);
	}

	public static MDStore newInstance(
		final String format,
		final String layout,
		final String interpretation,
		final String dsName,
		final String dsId,
		final String apiId,
		final String hdfsBasePath) {

		final String mdId = "md-" + UUID.randomUUID();

		final MDStore md = new MDStore();
		md.setId(mdId);
		md.setFormat(format);
		md.setLayout(layout);
		md.setInterpretation(interpretation);
		md.setCreationDate(new Date());
		md.setDatasourceName(dsName);
		md.setDatasourceId(dsId);
		md.setApiId(apiId);
		md.setHdfsPath(String.format("%s/%s", hdfsBasePath, mdId));

		return md;
	}

	@Override
	public String toString() {
		return String
			.format("MDStore [id=%s, format=%s, layout=%s, interpretation=%s, datasourceName=%s, datasourceId=%s, apiId=%s, hdfsPath=%s, creationDate=%s]", id, format, layout, interpretation, datasourceName, datasourceId, apiId, hdfsPath, creationDate);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) { return true; }
		if (!(obj instanceof MDStore)) { return false; }
		final MDStore other = (MDStore) obj;
		return Objects.equals(id, other.id);
	}

}
