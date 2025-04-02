package eu.dnetlib.oa.graph.usagestatsbuild.export;

import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
public abstract class LaReferenciaStats {

	private static final Logger logger = LoggerFactory.getLogger(LaReferenciaStats.class);

	public static void processLogs() throws Exception {
		try {
			logger.info("LaReferencia creating viewsStats");
			viewsStats();
			logger.info("LaReferencia created viewsStats");
			logger.info("LaReferencia creating downloadsStats");
			downloadsStats();
			logger.info("LaReferencia created downloadsStats");

		} catch (Exception e) {
			logger.error("Failed to process logs: ", e);
			throw new Exception("Failed to process logs: ", e);
		}
	}

	public static void viewsStats() throws Exception {

		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Creating la_result_views_monthly_tmp view");
		String sql = "CREATE OR REPLACE VIEW " + ConnectDB.getUsageStatsDBSchema() + ".la_result_views_monthly_tmp AS "
			+
			"SELECT entity_id AS id, COUNT(entity_id) as views, SUM(CASE WHEN referrer_name LIKE '%openaire%' " +
			"THEN 1 ELSE 0 END) AS openaire_referrer, " +
			"CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month, source " +
			"FROM " + ConnectDB.getUsageRawDataDBSchema() + ".lareferencialog where action='action' and " +
			"(source_item_type='oaItem' or source_item_type='repItem') " +
			"GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), " +
			"source ORDER BY source, entity_id";
		stmt.executeUpdate(sql);
		logger.info("Created la_result_views_monthly_tmp view");

		logger.info("Dropping la_views_stats_tmp table");
		sql = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".la_views_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("Dropped la_views_stats_tmp table");

		logger.info("Creating la_views_stats_tmp table");
		sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".la_views_stats_tmp " +
			"AS SELECT 'LaReferencia' as source, d.id as repository_id, ro.id as result_id, month as date, " +
			"max(views) AS count, max(openaire_referrer) AS openaire " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".la_result_views_monthly_tmp p, " +
			ConnectDB.getStatsDBSchema() + ".datasource_oids d, " + ConnectDB.getStatsDBSchema() + ".result_oids ro " +
			"WHERE p.source=d.oid AND p.id=ro.oid " +
			"GROUP BY d.id, ro.id, month " +
			"ORDER BY d.id, ro.id, month";
		stmt.executeUpdate(sql);
		logger.info("Created la_views_stats_tmp table");

		stmt.close();
	}

	private static void downloadsStats() throws Exception {

		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Creating la_result_downloads_monthly_tmp view");
		String sql = "CREATE OR REPLACE VIEW " + ConnectDB.getUsageStatsDBSchema()
			+ ".la_result_downloads_monthly_tmp AS " +
			"SELECT entity_id AS id, COUNT(entity_id) as downloads, SUM(CASE WHEN referrer_name LIKE '%openaire%' " +
			"THEN 1 ELSE 0 END) AS openaire_referrer, " +
			"CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month, source " +
			"FROM " + ConnectDB.getUsageRawDataDBSchema() + ".lareferencialog where action='download' and " +
			"(source_item_type='oaItem' or source_item_type='repItem') " +
			"GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), " +
			"source ORDER BY source, entity_id";
		stmt.executeUpdate(sql);
		logger.info("Created la_result_downloads_monthly_tmp view");

		logger.info("Dropping la_downloads_stats_tmp table");
		sql = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".la_downloads_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("Dropped la_downloads_stats_tmp table");

		logger.info("Creating la_downloads_stats_tmp table");
		sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".la_downloads_stats_tmp " +
			"AS SELECT 'LaReferencia' as source, d.id as repository_id, ro.id as result_id, month as date, " +
			"max(downloads) AS count, max(openaire_referrer) AS openaire " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".la_result_downloads_monthly_tmp p, " +
			ConnectDB.getStatsDBSchema() + ".datasource_oids d, " + ConnectDB.getStatsDBSchema() + ".result_oids ro " +
			"WHERE p.source=d.oid AND p.id=ro.oid " +
			"GROUP BY d.id, ro.id, month " +
			"ORDER BY d.id, ro.id, month";
		stmt.executeUpdate(sql);
		logger.info("Created la_downloads_stats_tmp table");

		stmt.close();
	}
}
