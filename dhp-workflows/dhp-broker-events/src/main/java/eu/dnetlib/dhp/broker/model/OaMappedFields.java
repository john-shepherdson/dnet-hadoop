
package eu.dnetlib.dhp.broker.model;

import java.io.Serializable;
import java.util.List;

public class OaMappedFields implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -7999704113195802008L;

	private String targetDatasourceId;
	private String targetDatasourceName;
	private String targetDatasourceType;
	private String targetResultId;
	private String targetResultTitle;
	private long targetDateofacceptance;
	private List<String> targetSubjects;
	private List<String> targetAuthors;
	private float trust;
	private String provenanceDatasourceId;
	private String provenanceDatasourceName;
	private String provenanceDatasourceType;
	private String provenanceResultId;

	public String getTargetDatasourceId() {
		return this.targetDatasourceId;
	}

	public void setTargetDatasourceId(final String targetDatasourceId) {
		this.targetDatasourceId = targetDatasourceId;
	}

	public String getTargetDatasourceName() {
		return this.targetDatasourceName;
	}

	public void setTargetDatasourceName(final String targetDatasourceName) {
		this.targetDatasourceName = targetDatasourceName;
	}

	public String getTargetDatasourceType() {
		return this.targetDatasourceType;
	}

	public void setTargetDatasourceType(final String targetDatasourceType) {
		this.targetDatasourceType = targetDatasourceType;
	}

	public String getTargetResultId() {
		return this.targetResultId;
	}

	public void setTargetResultId(final String targetResultId) {
		this.targetResultId = targetResultId;
	}

	public String getTargetResultTitle() {
		return this.targetResultTitle;
	}

	public void setTargetResultTitle(final String targetResultTitle) {
		this.targetResultTitle = targetResultTitle;
	}

	public long getTargetDateofacceptance() {
		return this.targetDateofacceptance;
	}

	public void setTargetDateofacceptance(final long targetDateofacceptance) {
		this.targetDateofacceptance = targetDateofacceptance;
	}

	public List<String> getTargetSubjects() {
		return this.targetSubjects;
	}

	public void setTargetSubjects(final List<String> targetSubjects) {
		this.targetSubjects = targetSubjects;
	}

	public List<String> getTargetAuthors() {
		return this.targetAuthors;
	}

	public void setTargetAuthors(final List<String> targetAuthors) {
		this.targetAuthors = targetAuthors;
	}

	public float getTrust() {
		return this.trust;
	}

	public void setTrust(final float trust) {
		this.trust = trust;
	}

	public String getProvenanceDatasourceId() {
		return this.provenanceDatasourceId;
	}

	public void setProvenanceDatasourceId(final String provenanceDatasourceId) {
		this.provenanceDatasourceId = provenanceDatasourceId;
	}

	public String getProvenanceDatasourceName() {
		return this.provenanceDatasourceName;
	}

	public void setProvenanceDatasourceName(final String provenanceDatasourceName) {
		this.provenanceDatasourceName = provenanceDatasourceName;
	}

	public String getProvenanceDatasourceType() {
		return this.provenanceDatasourceType;
	}

	public void setProvenanceDatasourceType(final String provenanceDatasourceType) {
		this.provenanceDatasourceType = provenanceDatasourceType;
	}

	public String getProvenanceResultId() {
		return this.provenanceResultId;
	}

	public void setProvenanceResultId(final String provenanceResultId) {
		this.provenanceResultId = provenanceResultId;
	}

}
