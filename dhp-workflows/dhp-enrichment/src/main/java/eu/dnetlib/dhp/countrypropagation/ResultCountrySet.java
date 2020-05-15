
package eu.dnetlib.dhp.countrypropagation;

import java.io.Serializable;
import java.util.ArrayList;

public class ResultCountrySet implements Serializable {
	private String resultId;
	private ArrayList<CountrySbs> countrySet;

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public ArrayList<CountrySbs> getCountrySet() {
		return countrySet;
	}

	public void setCountrySet(ArrayList<CountrySbs> countrySet) {
		this.countrySet = countrySet;
	}
}
