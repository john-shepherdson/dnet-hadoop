package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Objects;

public class H2020classification implements Serializable {
    private H2020Programme h2020Programme;
    private String level1;
    private String level2;
    private String level3;

    private String classification;


    public H2020Programme getH2020Programme() {
        return h2020Programme;
    }

    public void setH2020Programme(H2020Programme h2020Programme) {
        this.h2020Programme = h2020Programme;
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

    public String getClassification() {
        return classification;
    }

    public void setClassification(String classification) {
        this.classification = classification;
    }

    public void setLevels() {
        String[] tmp = classification.split(" $ ");
        level1 = tmp[0];
        if(tmp.length > 1){
            level2 = tmp[1];
        }
        if(tmp.length > 2){
            level3 = tmp[2];
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        H2020classification h2020classification = (H2020classification)o;

        return Objects.equals(level1, h2020classification.level1) &&
                Objects.equals(level2, h2020classification.level2) &&
                Objects.equals(level3, h2020classification.level3) &&
                Objects.equals(classification, h2020classification.classification) &&
                h2020Programme.equals(h2020classification.h2020Programme);
    }
}
