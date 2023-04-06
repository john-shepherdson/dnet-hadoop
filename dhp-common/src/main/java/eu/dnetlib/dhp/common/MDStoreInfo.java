
package eu.dnetlib.dhp.common;

/**
 * This utility represent the Metadata Store information
 * needed during the migration from mongo to HDFS to store
  */
public class MDStoreInfo {
	private String mdstore;
	private String currentId;
	private Long latestTimestamp;

	/**
	 * Instantiates a new Md store info.
	 */
	public MDStoreInfo() {
	}

	/**
	 * Instantiates a new Md store info.
	 *
	 * @param mdstore         the mdstore
	 * @param currentId       the current id
	 * @param latestTimestamp the latest timestamp
	 */
	public MDStoreInfo(String mdstore, String currentId, Long latestTimestamp) {
		this.mdstore = mdstore;
		this.currentId = currentId;
		this.latestTimestamp = latestTimestamp;
	}

	/**
	 * Gets mdstore.
	 *
	 * @return the mdstore
	 */
	public String getMdstore() {
		return mdstore;
	}

	/**
	 * Sets mdstore.
	 *
	 * @param mdstore the mdstore
	 * @return the mdstore
	 */
	public MDStoreInfo setMdstore(String mdstore) {
		this.mdstore = mdstore;
		return this;
	}

	/**
	 * Gets current id.
	 *
	 * @return the current id
	 */
	public String getCurrentId() {
		return currentId;
	}

	/**
	 * Sets current id.
	 *
	 * @param currentId the current id
	 * @return the current id
	 */
	public MDStoreInfo setCurrentId(String currentId) {
		this.currentId = currentId;
		return this;
	}

	/**
	 * Gets latest timestamp.
	 *
	 * @return the latest timestamp
	 */
	public Long getLatestTimestamp() {
		return latestTimestamp;
	}

	/**
	 * Sets latest timestamp.
	 *
	 * @param latestTimestamp the latest timestamp
	 * @return the latest timestamp
	 */
	public MDStoreInfo setLatestTimestamp(Long latestTimestamp) {
		this.latestTimestamp = latestTimestamp;
		return this;
	}

	@Override
	public String toString() {
		return "MDStoreInfo{" +
			"mdstore='" + mdstore + '\'' +
			", currentId='" + currentId + '\'' +
			", latestTimestamp=" + latestTimestamp +
			'}';
	}
}
