
package eu.dnetlib.dhp.bulktag.criteria;

import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

//Verifica tutti i componenti di una lista abbiano nel valore espresso dal jsonpath almeno uno dei valori espressi nel param
@VerbClass("forany_apply")
public class ForAnyApplyVerb implements Selection, JsonPathAware, ApplyOtherVerbAware, Serializable {
	private String jsonPath;
	private Object params ;
	private String applyVerb;

	public ForAnyApplyVerb() {
	}

	public ForAnyApplyVerb(final Object param) {
		this.params = param;
	}

	@Override
	public boolean apply(Object value) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
		if (applyVerb == null)
			throw new RuntimeException("The verb to be applied must be defined");
		VerbResolver resolver = new VerbResolver();
		Selection verbToApply = resolver.getSelectionCriteria(applyVerb, params);

		List<Object> parsed = getParsed(value); //il valore che ottengo dai metadati
		if (parsed == null) return false;

		return parsed.stream().anyMatch(o -> {

			if (verbToApply instanceof JsonPathAware jsonPathAwareVerb) {
				jsonPathAwareVerb.setJsonPath(jsonPath);
                try {
                    return verbToApply.apply(o);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                }
            }
			if(jsonPath == null) {
				try {
					return verbToApply.apply(o);
				} catch (IOException e) {
					throw new RuntimeException(e);
				} catch (InvocationTargetException e) {
					throw new RuntimeException(e);
				} catch (NoSuchMethodException e) {
					throw new RuntimeException(e);
				} catch (IllegalAccessException e) {
					throw new RuntimeException(e);
				} catch (InstantiationException e) {
					throw new RuntimeException(e);
				}
			}else{
				ReadContext ctx;
				if(o instanceof String)
					ctx = JsonPath.parse((String)o);
				else
					ctx = JsonPath.parse(o);
				Object results;
				try{
					results = ctx.read(jsonPath);
					return verbToApply.apply(results);
				}catch(PathNotFoundException e){
					return false;
				} catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                }
            }
        });


	}

	private static @Nullable List<Object> getParsed(Object value) {
		List<Object> parsed = null;
		if(value instanceof List<?> lista)
			parsed = (List<Object>) lista;
		else {
			try {
				parsed = new ObjectMapper().readValue((String) value, List.class);
			} catch (IOException e) {
				return null;
			}
		}

		if(parsed.isEmpty())
			return null;
		return parsed;
	}

	@Override
	public boolean apply(Object value, Object otherEntityValue) throws IOException {
		List<Object> parsed = getParsed(value);
		return parsed.stream().allMatch(o -> {
			ExistVerb exist = new ExistVerb(params);
			exist.setJsonPath(jsonPath);
            try {
                return exist.apply(o, otherEntityValue);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
	}




	public Object getParam() {
		return params;
	}

	public void setParam(Object param) {
		this.params = param;
	}



	@Override
	public void setApplyVerb(String applyVerb) {
		this.applyVerb = applyVerb;
	}


	@Override
	public void setJsonPath(String jsonpath) {
		this.jsonPath = jsonpath;
	}
}
