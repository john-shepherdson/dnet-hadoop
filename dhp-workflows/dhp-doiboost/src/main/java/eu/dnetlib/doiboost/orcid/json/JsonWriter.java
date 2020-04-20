package eu.dnetlib.doiboost.orcid.json;

import com.google.gson.JsonObject;

import eu.dnetlib.doiboost.orcid.model.AuthorData;


public class JsonWriter {

	public static String create(AuthorData authorData) {
		JsonObject author = new JsonObject();
		author.addProperty("oid", authorData.getOid());
		author.addProperty("name", authorData.getName());
		author.addProperty("surname", authorData.getSurname());
		if (authorData.getCreditName()!=null) {
			author.addProperty("creditname", authorData.getCreditName());
		}
		return author.toString();
	}
}
