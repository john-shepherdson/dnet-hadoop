
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;
import java.util.Optional;

/**
 * To describe the funded amount. It has the following parameters: - private String currency to store the currency of
 * the fund - private float totalcost to store the total cost of the project - private float fundedamount to store the
 * funded amount by the funder
 */
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
