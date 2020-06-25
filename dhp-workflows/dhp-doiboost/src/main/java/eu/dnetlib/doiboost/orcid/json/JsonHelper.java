
package eu.dnetlib.doiboost.orcid.json;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.dnetlib.doiboost.orcidnodoi.model.WorkDataNoDoi;

public class JsonHelper {

	public static String createOidWork(WorkDataNoDoi workData) {
		JsonObject oidWork = new JsonObject();
		oidWork.addProperty("oid", workData.getOid());
		oidWork.addProperty("work", new Gson().toJson(workData));
		return oidWork.toString();
	}
}
