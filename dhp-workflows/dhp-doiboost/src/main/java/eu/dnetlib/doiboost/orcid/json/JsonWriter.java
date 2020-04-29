package eu.dnetlib.doiboost.orcid.json;

import com.google.gson.JsonObject;
import eu.dnetlib.doiboost.orcid.model.AuthorData;
import eu.dnetlib.doiboost.orcid.model.WorkData;

public class JsonWriter {

  public static String create(AuthorData authorData) {
    JsonObject author = new JsonObject();
    author.addProperty("oid", authorData.getOid());
    author.addProperty("name", authorData.getName());
    author.addProperty("surname", authorData.getSurname());
    if (authorData.getCreditName() != null) {
      author.addProperty("creditname", authorData.getCreditName());
    }
    return author.toString();
  }

  public static String create(WorkData workData) {
    JsonObject work = new JsonObject();
    work.addProperty("oid", workData.getOid());
    work.addProperty("doi", workData.getDoi());
    return work.toString();
  }
}
