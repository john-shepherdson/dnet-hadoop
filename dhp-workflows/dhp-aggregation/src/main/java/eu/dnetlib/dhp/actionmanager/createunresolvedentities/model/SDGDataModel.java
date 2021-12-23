package eu.dnetlib.dhp.actionmanager.createunresolvedentities.model;

import com.opencsv.bean.CsvBindByPosition;

import java.io.Serializable;

public class SDGDataModel implements Serializable{

    @CsvBindByPosition(position = 0)
//    @CsvBindByName(column = "doi")
    private String doi;

    @CsvBindByPosition(position = 1)
//    @CsvBindByName(column = "sdg")
    private String sbj;


    public SDGDataModel() {

    }

    public SDGDataModel(String doi, String sbj) {
        this.doi = doi;
        this.sbj = sbj;

    }

    public static SDGDataModel newInstance(String d, String sbj) {
        return new SDGDataModel(d, sbj);
    }

    public String getDoi() {
        return doi;
    }

    public void setDoi(String doi) {
        this.doi = doi;
    }


    public String getSbj() {
        return sbj;
    }

    public void setSbj(String sbj) {
        this.sbj = sbj;
    }
}
