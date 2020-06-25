
package eu.dnetlib.dhp.broker.model;

import java.io.Serializable;
import java.util.List;

public class MappedFields implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -7999704113195802008L;

	private String targetDatasourceId;
	private String targetDatasourceName;
	private String targetResultId;
	private String targetResultTitle;
	private long targetDateofacceptance;
	private List<String> targetSubjects;
	private List<String> targetAuthors;
	private float trust;
	private String provenanceDatasourceId;
	private String provenanceDatasourceName;
	private String provenanceResultId;

	public String getTargetDatasourceId() {
		return targetDatasourceId;
	}

	public void setTargetDatasourceId(final String targetDatasourceId) {
		this.targetDatasourceId = targetDatasourceId;
	}

	public String getTargetDatasourceName() {
		return targetDatasourceName;
	}

	public void setTargetDatasourceName(final String targetDatasourceName) {
		this.targetDatasourceName = targetDatasourceName;
	}

	public String getTargetResultId() {
		return targetResultId;
	}

	public void setTargetResultId(final String targetResultId) {
		this.targetResultId = targetResultId;
	}

	public String getTargetResultTitle() {
		return targetResultTitle;
	}

	public void setTargetResultTitle(final String targetResultTitle) {
		this.targetResultTitle = targetResultTitle;
	}

	public long getTargetDateofacceptance() {
		return targetDateofacceptance;
	}

	public void setTargetDateofacceptance(final long targetDateofacceptance) {
		this.targetDateofacceptance = targetDateofacceptance;
	}

	public List<String> getTargetSubjects() {
		return targetSubjects;
	}

	public void setTargetSubjects(final List<String> targetSubjects) {
		this.targetSubjects = targetSubjects;
	}

	public List<String> getTargetAuthors() {
		return targetAuthors;
	}

	public void setTargetAuthors(final List<String> targetAuthors) {
		this.targetAuthors = targetAuthors;
	}

	public float getTrust() {
		return trust;
	}

	public void setTrust(final float trust) {
		this.trust = trust;
	}

	public String getProvenanceDatasourceId() {
		return provenanceDatasourceId;
	}

	public void setProvenanceDatasourceId(final String provenanceDatasourceId) {
		this.provenanceDatasourceId = provenanceDatasourceId;
	}

	public String getProvenanceDatasourceName() {
		return provenanceDatasourceName;
	}

	public void setProvenanceDatasourceName(final String provenanceDatasourceName) {
		this.provenanceDatasourceName = provenanceDatasourceName;
	}

	public String getProvenanceResultId() {
		return provenanceResultId;
	}

	public void setProvenanceResultId(final String provenanceResultId) {
		this.provenanceResultId = provenanceResultId;
	}

}
