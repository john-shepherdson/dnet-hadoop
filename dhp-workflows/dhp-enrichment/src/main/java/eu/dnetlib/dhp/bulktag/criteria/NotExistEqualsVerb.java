
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@VerbClass("not_equals")
public class NotExistEqualsVerb implements Selection, Serializable {

    private Object params ;

//	public NotEqualVerb(final String param) {
//		this.params = List.of(param);
//	}
//	public NotEqualVerb(final List<String> param) {
//		this.params = param;
//	}

    public NotExistEqualsVerb(final Object param) {
        this.params = param;
//		if(param instanceof String s)
//			this.params = List.of(s);
//		if(param instanceof List<?> lista && !lista.isEmpty() && lista.get(0) instanceof String)
//			lista.forEach(l -> params.add(String.valueOf(l)));
    }
    public NotExistEqualsVerb() {
    }

    public Object getParam() {
        return params;
    }

    public void setParam(Object param) {
        this.params = param;
    }

    @Override
    public boolean apply(Object value) {
        if(params instanceof String sParam){
            if(value instanceof String s  )
                return !s.trim().equals(sParam.trim());
            if (value instanceof List<?> lista )
                return lista.stream().allMatch(l -> (l instanceof String sValue && !sValue.trim().equals(sParam.trim())));
        }

        throw new RuntimeException("Verb not applicable with these parameters");
    }

    @Override
    public boolean apply(Object value, Object otherEntityValue) throws IOException {
        return false;
    }
}
