
package eu.dnetlib.dhp.bypassactionset.model;

import java.io.Serializable;

import com.opencsv.bean.CsvBindByPosition;

public class FOSDataModel implements Serializable {
	@CsvBindByPosition(position = 1)
//    @CsvBindByName(column = "doi")
	private String doi;

	@CsvBindByPosition(position = 2)
//    @CsvBindByName(column = "level1")
	private String level1;

	@CsvBindByPosition(position = 3)
//    @CsvBindByName(column = "level2")
	private String level2;

	@CsvBindByPosition(position = 4)
//    @CsvBindByName(column = "level3")
	private String level3;

	public FOSDataModel() {

	}

	public FOSDataModel(String doi, String level1, String level2, String level3) {
		this.doi = doi;
		this.level1 = level1;
		this.level2 = level2;
		this.level3 = level3;
	}

	public static FOSDataModel newInstance(String d, String level1, String level2, String level3) {
		return new FOSDataModel(d, level1, level2, level3);
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
