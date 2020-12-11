
package eu.dnetlib.dhp.schema.orcid;

public class OrcidData {
	protected String base64CompressData;
	protected String statusCode;
	protected String downloadDate;

	public String getBase64CompressData() {
		return base64CompressData;
	}

	public void setBase64CompressData(String base64CompressData) {
		this.base64CompressData = base64CompressData;
	}

	public String getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(String statusCode) {
		this.statusCode = statusCode;
	}

	public String getDownloadDate() {
		return downloadDate;
	}

	public void setDownloadDate(String downloadDate) {
		this.downloadDate = downloadDate;
	}
}
