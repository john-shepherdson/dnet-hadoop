
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

public class H2020Classification implements Serializable {
	private Programme programme;

	private String level1;
	private String level2;
	private String level3;

	private String classification;

	public Programme getProgramme() {
		return programme;
	}

	public void setProgramme(Programme programme) {
		this.programme = programme;
	}

	public String getLevel1() {
		return level1;
	}

	public void setLevel1(String level1) {
		this.level1 = level1;
	}

	public String getLevel2() {
		return level2;
	}

	public void setLevel2(String level2) {
		this.level2 = level2;
	}

	public String getLevel3() {
		return level3;
	}

	public void setLevel3(String level3) {
		this.level3 = level3;
	}

	public String getClassification() {
		return classification;
	}

	public void setClassification(String classification) {
		this.classification = classification;
	}

	public static H2020Classification newInstance(String programme_code, String programme_description, String level1,
		String level2, String level3, String classification) {
		H2020Classification h2020classification = new H2020Classification();
		h2020classification.programme = Programme.newInstance(programme_code, programme_description);
		h2020classification.level1 = level1;
		h2020classification.level2 = level2;
		h2020classification.level3 = level3;
		h2020classification.classification = classification;
		return h2020classification;
	}
}
