package eu.dnetlib.dhp.bulktag.criteria;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import eu.dnetlib.dhp.bulktag.criteria.VerbClass;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * Verifica che una data sia successiva ad un'altra.
 * Usato ad esempio per: publicationDate > project.startDate
 */
//todo rivedere questa classe
@VerbClass("after")
public class AfterVerb implements Selection, JsonPathAware, Serializable {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private Object param; //il value nella configurazione
    private String jsonPath;

    public AfterVerb() {}

    public AfterVerb(Object param) {
        this.param = param;
    }

    @Override
    public boolean apply(Object value) { //qua devo passare il valore da confrontare
       return false;
    }

    @Override
    public boolean apply(Object value, Object otherEntityValue) throws IOException {
        if (value == null || (param == null && jsonPath == null)) {
            return false;
        }

        try {
            ReadContext ctx;
            ctx = JsonPath.parse((String)value);
            // estraggo i valori usando il jsonpath
            Object results = ctx.read(jsonPath);
            if(results == null)
                return false;

            LocalDate resultDate = LocalDate.of(1900, 1, 1);
            if(results instanceof String date)
                 resultDate = parseDate(date);
            else
                return false;

            if(jsonPath != null && param != null && otherEntityValue != null){
                if ( param instanceof List<?>) {
                    return false;
                }
                    LocalDate projectDate = parseDate(otherEntityValue);
                    LocalDate thresholdDate = parseDate(param);
                    return resultDate.isAfter(projectDate) && resultDate.isAfter(thresholdDate);

            }
            if(jsonPath != null ){

                    LocalDate projectDate = parseDate(otherEntityValue);
                    return resultDate.isAfter(projectDate) ;


            }
            if( param != null)
                return apply(value);

            return false;
        } catch (Exception e) {
            System.err.println("AfterVerb: invalid comparison " + e.getMessage());
            return false;
        }
    }

    private LocalDate parseDate(Object obj) {
        if (obj instanceof LocalDate d) {
            return d;
        }
        if (obj instanceof String s) {
            return LocalDate.parse(s, formatter);
        }
        throw new IllegalArgumentException("Unsupported date format: " + obj);
    }

    @SuppressWarnings("unchecked")
    private LocalDate extractDateFromParam(Object obj) {
        if (obj instanceof String s) {
            return LocalDate.parse(s, formatter);
        }
        if (obj instanceof Map<?,?> map && map.containsKey("startDate")) {
            return LocalDate.parse((String) map.get("startDate"), formatter);
        }
        throw new IllegalArgumentException("Unsupported param format: " + obj);
    }

    public Object getParam() { return param; }
    public void setParam(Object param) { this.param = param; }

    @Override
    public void setJsonPath(String jsonpath) {
        this.jsonPath = jsonpath;
    }
}
