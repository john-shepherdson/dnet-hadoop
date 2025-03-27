
package eu.dnetlib.dhp.monitoring;

import org.bson.Document;

public class AggregationLog {

	private long time;
	private String wfName;
	private String sinkSize;
	private String dataProviderId;
	private String dataProviderInterface;
	private String dataProviderName;
	private String dateOfCollection;
	private String namespacePrefix;
	private String completedSuccessfully;
	private String fetchOriginalsMdId;
	private String selectModeSelection;
	private String storeRecordsMdId;
	private String updateInfoMdId;
	private String storeRefreshMdId;
	private String endDate;
	private String storeMdId;
	private String startDate;

	public AggregationLog() {
	}

	public static AggregationLog fromMongoDocument(final Document document) {
		final AggregationLog aggregationLog = new AggregationLog();
		aggregationLog.setTime(document.getLong("log:date"));
		aggregationLog.setWfName(document.getString("system:wfName"));
		aggregationLog.setSinkSize(document.getString("mainlog:sinkSize"));
		aggregationLog.setDataProviderId(document.getString("dataprovider:id"));
		aggregationLog.setDataProviderInterface(document.getString("dataprovider:interface"));
		aggregationLog.setDataProviderName(document.getString("dataprovider:name"));
		aggregationLog.setDateOfCollection(document.getString("dateOfCollection"));
		aggregationLog.setNamespacePrefix(document.getString("namespacePrefix"));
		aggregationLog.setCompletedSuccessfully(document.getString("system:isCompletedSuccessfully"));
		aggregationLog.setFetchOriginalsMdId(document.getString("system:node:fetchOriginals:mdId"));
		aggregationLog.setSelectModeSelection(document.getString("system:node:SELECT_MODE:selection"));
		aggregationLog.setStoreRecordsMdId(document.getString("system:node:storeRecords:mdId"));
		aggregationLog.setUpdateInfoMdId(document.getString("system:node:UPDATE_INFO:mdId"));
		aggregationLog.setStoreRefreshMdId(document.getString("system:node:STORE_REFRESH:mdId"));
		aggregationLog.setEndDate(document.getString("system:endDate"));
		aggregationLog.setStoreMdId(document.getString("system:node:STORE:mdId"));
		aggregationLog.setStartDate(document.getString("system:startDate"));
		return aggregationLog;
	}

	@Override
	public String toString() {
		return "AggregationLog {" +
			"time=" + time +
			", wfName='" + wfName + '\'' +
			", sinkSize='" + sinkSize + '\'' +
			", dataProviderId='" + dataProviderId + '\'' +
			", dataProviderInterface='" + dataProviderInterface + '\'' +
			", dataProviderName='" + dataProviderName + '\'' +
			", dateOfCollection='" + dateOfCollection + '\'' +
			", namespacePrefix='" + namespacePrefix + '\'' +
			", isCompletedSuccessfully=" + completedSuccessfully +
			", fetchOriginalsMdId='" + fetchOriginalsMdId + '\'' +
			", selectModeSelection='" + selectModeSelection + '\'' +
			", storeRecordsMdId='" + storeRecordsMdId + '\'' +
			", updateInfoMdId='" + updateInfoMdId + '\'' +
			", storeRefreshMdId='" + storeRefreshMdId + '\'' +
			", endDate='" + endDate + '\'' +
			", storeMdId='" + storeMdId + '\'' +
			", startDate='" + startDate + '\'' +
			'}';

	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String getWfName() {
		return wfName;
	}

	public void setWfName(String wfName) {
		this.wfName = wfName;
	}

	public String getSinkSize() {
		return sinkSize;
	}

	public void setSinkSize(String sinkSize) {
		this.sinkSize = sinkSize;
	}

	public String getDataProviderId() {
		return dataProviderId;
	}

	public void setDataProviderId(String dataProviderId) {
		this.dataProviderId = dataProviderId;
	}

	public String getDataProviderInterface() {
		return dataProviderInterface;
	}

	public void setDataProviderInterface(String dataProviderInterface) {
		this.dataProviderInterface = dataProviderInterface;
	}

	public String getDataProviderName() {
		return dataProviderName;
	}

	public void setDataProviderName(String dataProviderName) {
		this.dataProviderName = dataProviderName;
	}

	public String getDateOfCollection() {
		return dateOfCollection;
	}

	public void setDateOfCollection(String dateOfCollection) {
		this.dateOfCollection = dateOfCollection;
	}

	public String getNamespacePrefix() {
		return namespacePrefix;
	}

	public void setNamespacePrefix(String namespacePrefix) {
		this.namespacePrefix = namespacePrefix;
	}

	public String getCompletedSuccessfully() {
		return completedSuccessfully;
	}

	public void setCompletedSuccessfully(String completedSuccessfully) {
		this.completedSuccessfully = completedSuccessfully;
	}

	public String getFetchOriginalsMdId() {
		return fetchOriginalsMdId;
	}

	public void setFetchOriginalsMdId(String fetchOriginalsMdId) {
		this.fetchOriginalsMdId = fetchOriginalsMdId;
	}

	public String getSelectModeSelection() {
		return selectModeSelection;
	}

	public void setSelectModeSelection(String selectModeSelection) {
		this.selectModeSelection = selectModeSelection;
	}

	public String getStoreRecordsMdId() {
		return storeRecordsMdId;
	}

	public void setStoreRecordsMdId(String storeRecordsMdId) {
		this.storeRecordsMdId = storeRecordsMdId;
	}

	public String getUpdateInfoMdId() {
		return updateInfoMdId;
	}

	public void setUpdateInfoMdId(String updateInfoMdId) {
		this.updateInfoMdId = updateInfoMdId;
	}

	public String getStoreRefreshMdId() {
		return storeRefreshMdId;
	}

	public void setStoreRefreshMdId(String storeRefreshMdId) {
		this.storeRefreshMdId = storeRefreshMdId;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public String getStoreMdId() {
		return storeMdId;
	}

	public void setStoreMdId(String storeMdId) {
		this.storeMdId = storeMdId;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
}
