
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

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
