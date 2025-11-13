package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Verifica che una data sia precedente ad un'altra.
 * Usato ad esempio per: publicationDate > project.startDate
 * Si applica a due oggetti di tipo stringa che rappresentano le date
 */
//todo rivedere questa classe
@VerbClass("apply_regexp")
public class ApplyRegexpVerb implements Selection, Serializable {

    private Object param; //il value nella configurazione


    public ApplyRegexpVerb() {}

    public ApplyRegexpVerb(Object param) {
        this.param = param;
    } //param e' la regexp

    @Override
    public boolean apply(Object value) { //qua devo passare il valore da confrontare
        if(!(param instanceof String))
            throw new RuntimeException("The regexp must be passed as a String");
        final String regexp = (String) param;
        if(value != null && regexp != null ) {
            if(value instanceof String stringValue)
                return stringValue.matches(regexp);
            if (value instanceof List<?> lista){
                Boolean ret = lista.stream().anyMatch(l -> l instanceof String stringValue && stringValue.matches(regexp));
                return  ret;
            }

            else
                throw new RuntimeException("The value obtained from metadata is not of type String");

        }
        throw new RuntimeException("At least one comparison value is missing");

    }

    @Override
    public boolean apply(Object value, Object otherEntityValue) throws IOException {


        throw new RuntimeException("Not applicable for compariso value");


    }


    public Object getParam() { return param; }
    public void setParam(Object param) { this.param = param; }


}
