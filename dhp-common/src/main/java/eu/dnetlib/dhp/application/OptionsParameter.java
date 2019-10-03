package eu.dnetlib.dhp.application;


public class OptionsParameter {

    private String paramName;
    private String paramLongName;
    private String paramDescription;
    private boolean paramRequired;

    public OptionsParameter() {
    }

    public String getParamName() {
        return paramName;
    }

    public String getParamLongName() {
        return paramLongName;
    }

    public String getParamDescription() {
        return paramDescription;
    }

    public boolean isParamRequired() {
        return paramRequired;
    }
}
