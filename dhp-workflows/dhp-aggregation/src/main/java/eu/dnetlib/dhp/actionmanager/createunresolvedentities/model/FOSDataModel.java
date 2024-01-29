
package eu.dnetlib.dhp.actionmanager.createunresolvedentities.model;

import java.io.Serializable;

import com.opencsv.bean.CsvBindByPosition;

public class FOSDataModel implements Serializable {
	@CsvBindByPosition(position = 0)
//    @CsvBindByName(column = "doi")
	private String doi;

	@CsvBindByPosition(position = 1)
//    @CsvBindByName(column = "doi")
	private String oaid;
	@CsvBindByPosition(position = 2)
//    @CsvBindByName(column = "level1")
	private String level1;

	@CsvBindByPosition(position = 3)
//    @CsvBindByName(column = "level2")
	private String level2;

	@CsvBindByPosition(position = 4)
//    @CsvBindByName(column = "level3")
	private String level3;

	@CsvBindByPosition(position = 5)
//    @CsvBindByName(column = "level3")
	private String level4;
	@CsvBindByPosition(position = 6)
	private String scoreL3;
	@CsvBindByPosition(position = 7)
	private String scoreL4;

	public FOSDataModel() {

	}

	public FOSDataModel(String doi, String level1, String level2, String level3, String level4, String l3score,
		String l4score) {
		this.doi = doi;
		this.level1 = level1;
		this.level2 = level2;
		this.level3 = level3;
		this.level4 = level4;
		this.scoreL3 = l3score;
		this.scoreL4 = l4score;
	}

	public FOSDataModel(String doi, String level1, String level2, String level3) {
		this.doi = doi;
		this.level1 = level1;
		this.level2 = level2;
		this.level3 = level3;
	}

	public static FOSDataModel newInstance(String d, String level1, String level2, String level3, String level4,
		String scorel3, String scorel4) {
		return new FOSDataModel(d, level1, level2, level3, level4, scorel3, scorel4);
	}

	public String getOaid() {
		return oaid;
	}

	public void setOaid(String oaid) {
		this.oaid = oaid;
	}

	public String getLevel4() {
		return level4;
	}

	public void setLevel4(String level4) {
		this.level4 = level4;
	}

	public String getScoreL3() {
		return scoreL3;
	}

	public void setScoreL3(String scoreL3) {
		this.scoreL3 = scoreL3;
	}

	public String getScoreL4() {
		return scoreL4;
	}

	public void setScoreL4(String scoreL4) {
		this.scoreL4 = scoreL4;
	}

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
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
}
