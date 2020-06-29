
package eu.dnetlib.doiboost.orcid.json;

import com.google.gson.Gson;

import eu.dnetlib.doiboost.orcidnodoi.model.WorkDataNoDoi;

public class JsonHelper {

	public static String createOidWork(WorkDataNoDoi workData) {
		return new Gson().toJson(workData);
	}
}
