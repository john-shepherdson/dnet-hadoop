package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Verifica che una data sia precedente ad un'altra.
 * Usato ad esempio per: publicationDate < project.startDate
 * Si applica a due oggetti di tipo stringa che rappresentano le date
 */

@VerbClass("before")
public class BeforeVerb implements Selection, Serializable {

    private Object param; //il value nella configurazione


    public BeforeVerb() {}

    public BeforeVerb(Object param) {
        this.param = param;
    }

    @Override
    public boolean apply(Object value) { //qua devo passare il valore da confrontare
        LocalDate resultDate;
        if(value != null && param != null) {
            if(value instanceof String date)
                resultDate = parseDate(date);
            else
                throw new RuntimeException("The value obtained from metadata is not of type String");
            LocalDate thresholdDate = parseDate(param);
            return resultDate.isBefore(thresholdDate);
        }
        throw new RuntimeException("At least one comparison value is missing");
    }

    @Override
    public boolean apply(Object value, Object otherEntityValue) throws IOException {
        LocalDate resultDate ;
        if(value != null && otherEntityValue != null){

            if(value instanceof String date)
                resultDate = parseDate(date);
            else
                return false;
            LocalDate projectDate = parseDate(otherEntityValue);
            return resultDate.isBefore(projectDate);
        }
        if (value != null && otherEntityValue == null)
            return apply(value);

        throw new RuntimeException("At least one comparison value is missing ");


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
