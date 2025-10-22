package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Verifica che una data sia successiva ad un'altra.
 * Usato ad esempio per: publicationDate > project.startDate
 */
//todo rivedere questa classe
@VerbClass("before")
public class BeforeVerb implements Selection, Serializable {

    private Object param; //il value nella configurazione


    public BeforeVerb() {}

    public BeforeVerb(Object param) {
        this.param = param;
    }

    @Override
    public boolean apply(Object value) { //qua devo passare il valore da confrontare
       return false;
    }

    @Override
    public boolean apply(Object value, Object otherEntityValue) throws IOException {
        LocalDate resultDate = LocalDate.of(1900, 1, 1);
        if(value != null && otherEntityValue != null){

            if(value instanceof String date)
                resultDate = parseDate(date);
            else
                return false;
            LocalDate projectDate = parseDate(otherEntityValue);
            return resultDate.isBefore(projectDate);
        }
        if(value != null && param != null) {
            if(value instanceof String date)
                resultDate = parseDate(date);
            else
                return false;
            LocalDate thresholdDate = parseDate(param);
            return resultDate.isBefore(thresholdDate);
        }
        return false;


    }

    private LocalDate parseDate(Object obj) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        if (obj instanceof LocalDate d) {
            return d;
        }
        if (obj instanceof String s) {
            return LocalDate.parse(s, formatter);
        }
        throw new IllegalArgumentException("Unsupported date format: " + obj);
    }


    public Object getParam() { return param; }
    public void setParam(Object param) { this.param = param; }


}
