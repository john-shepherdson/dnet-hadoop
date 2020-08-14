
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

/**
 * Used to refer to the Article Processing Charge information. Not dumped in this release. It contains two parameters: -
 * currency of type String to store the currency of the APC - amount of type String to stores the charged amount
 */
public class APC implements Serializable {
	private String currency;
	private String amount;

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public String getAmount() {
		return amount;
	}

	public void setAmount(String amount) {
		this.amount = amount;
	}
}
