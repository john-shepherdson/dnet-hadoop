package eu.dnetlib.dhp.bulktag.criteria;

import eu.dnetlib.dhp.bulktag.criteria.VerbClass;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Verifica che una data sia successiva ad un'altra.
 * Usato ad esempio per: publicationDate > project.startDate
 */
//todo rivedere questa classe
@VerbClass("after")
public class AfterVerb implements Selection, JsonPathAware, Serializable {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private Object param;
    private String jsonPath;

    public AfterVerb() {}

    public AfterVerb(Object param) {
        this.param = param;
    }

    @Override
    public boolean apply(Object value) { //qua devo passare il valore da confrontare
        if (value == null || (param == null && jsonPath == null)) {
            return false;
        }

        try {
            // Caso 1: value è già una stringa data
            LocalDate left = parseDate(value);

            // Caso 2: param può essere una stringa o un oggetto (es. Map con "startDate")
            LocalDate right = extractDateFromParam(param);

            return left.isAfter(right);

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

    }
}
