
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;
import java.util.Optional;

public class Granted implements Serializable {
	private String currency;
	private float totalcost;
	private float fundedamount;

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public float getTotalcost() {
		return totalcost;
	}

	public void setTotalcost(float totalcost) {
		this.totalcost = totalcost;
	}

	public float getFundedamount() {
		return fundedamount;
	}

	public void setFundedamount(float fundedamount) {
		this.fundedamount = fundedamount;
	}

	public static Granted newInstance(String currency, float totalcost, float fundedamount) {
		Granted granted = new Granted();
		granted.currency = currency;
		granted.totalcost = totalcost;
		granted.fundedamount = fundedamount;
		return granted;
	}

	public static Granted newInstance(String currency, float fundedamount) {
		Granted granted = new Granted();
		granted.currency = currency;
		granted.fundedamount = fundedamount;
		return granted;
	}
}
