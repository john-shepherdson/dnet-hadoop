
package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Objects;

/**
 * To store information about the classification for the project. The classification depends on the programme. For example
 * H2020-EU.3.4.5.3 can be classified as
 * H2020-EU.3.       => Societal Challenges  (level1)
 * H2020-EU.3.4.     => Transport            (level2)
 * H2020-EU.3.4.5.   => CLEANSKY2            (level3)
 * H2020-EU.3.4.5.3. => IADP Fast Rotorcraft (level4)
 *
 * We decided to explicitly represent up to three levels in the classification.
 *
 * H2020Classification has the following parameters:
 * - private Programme programme to store the information about the programme related to this classification
 * - private String level1 to store the information about the level 1 of the classification (Priority or Pillar of the EC)
 * - private String level2 to store the information about the level2 af the classification (Objectives (?))
 * - private String level3 to store the information about the level3 of the classification
 * - private String classification to store the entire classification related to the programme
 */

public class H2020Classification implements Serializable {
	private H2020Programme h2020Programme;
	private String level1;
	private String level2;
	private String level3;

	private String classification;

	public H2020Programme getH2020Programme() {
		return h2020Programme;
	}

	public void setH2020Programme(H2020Programme h2020Programme) {
		this.h2020Programme = h2020Programme;
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

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		H2020Classification h2020classification = (H2020Classification) o;

		return Objects.equals(level1, h2020classification.level1) &&
			Objects.equals(level2, h2020classification.level2) &&
			Objects.equals(level3, h2020classification.level3) &&
			Objects.equals(classification, h2020classification.classification) &&
			h2020Programme.equals(h2020classification.h2020Programme);
	}
}
