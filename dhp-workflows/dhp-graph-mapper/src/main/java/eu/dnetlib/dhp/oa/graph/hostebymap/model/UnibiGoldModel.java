package eu.dnetlib.dhp.oa.graph.hostebymap.model;

import com.opencsv.bean.CsvBindByName;

import java.io.Serializable;

public class UnibiGoldModel implements Serializable {
    @CsvBindByName(column = "ISSN")
    private String issn;
    @CsvBindByName(column = "ISSN_L")
    private String issn_l;
    @CsvBindByName(column = "TITLE")
    private String title;
    @CsvBindByName(column = "TITLE_SOURCE")
    private String title_source;

    public String getIssn() {
        return issn;
    }

    public void setIssn(String issn) {
        this.issn = issn;
    }

    public String getIssn_l() {
        return issn_l;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitle_source() {
        return title_source;
    }

    public void setTitle_source(String title_source) {
        this.title_source = title_source;
    }
}
