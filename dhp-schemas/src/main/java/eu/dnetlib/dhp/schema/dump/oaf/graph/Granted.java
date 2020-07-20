package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

public class Granted implements Serializable {
    private String currency;
    private String totalcost;
    private String fundedamount;

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getTotalcost() {
        return totalcost;
    }

    public void setTotalcost(String totalcost) {
        this.totalcost = totalcost;
    }

    public String getFundedamount() {
        return fundedamount;
    }

    public void setFundedamount(String fundedamount) {
        this.fundedamount = fundedamount;
    }
}
