
package eu.dnetlib.doiboost.orcid.model;

import java.io.Serializable;

import org.apache.hadoop.io.Text;

import com.google.gson.JsonObject;

import scala.Tuple2;

public class DownloadedRecordData implements Serializable {

	private String orcidId;
	private String statusCode;
	private String compressedData;
	private String errorMessage;

	public Tuple2<String, String> toTuple2() {
		JsonObject data = new JsonObject();
		data.addProperty("statusCode", getStatusCode());
		if (getCompressedData() != null) {
			data.addProperty("compressedData", getCompressedData());
		}
		if (getErrorMessage() != null) {
			data.addProperty("errorMessage", getErrorMessage());
		}
		return new Tuple2<>(orcidId, data.toString());
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getOrcidId() {
		return orcidId;
	}

	public void setOrcidId(String orcidId) {
		this.orcidId = orcidId;
	}

	public int getStatusCode() {
		return Integer.parseInt(statusCode);
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = Integer.toString(statusCode);
	}

	public String getCompressedData() {
		return compressedData;
	}

	public void setCompressedData(String compressedData) {
		this.compressedData = compressedData;
	}

}
