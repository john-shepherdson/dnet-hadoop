
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.Result;

public class ResultWithRelations implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -1368401915974311571L;

	private Result result;

	private final List<RelatedDataset> datasets = new ArrayList<>();
	private final List<RelatedPublication> publications = new ArrayList<>();
	private final List<RelatedSoftware> softwares = new ArrayList<>();
	private final List<RelatedProject> projects = new ArrayList<>();

	public ResultWithRelations() {
	}

	public ResultWithRelations(final Result result) {
		this.result = result;
	}

	public Result getResult() {
		return result;
	}

	public List<RelatedDataset> getDatasets() {
		return datasets;
	}

	public List<RelatedPublication> getPublications() {
		return publications;
	}

	public List<RelatedSoftware> getSoftwares() {
		return softwares;
	}

	public List<RelatedProject> getProjects() {
		return projects;
	}

	public void setResult(final Result result) {
		this.result = result;
	}

}
