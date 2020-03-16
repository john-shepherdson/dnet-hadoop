package eu.dnetlib.dhp.migration.utils;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DbClient implements Closeable {

	private static final Log log = LogFactory.getLog(DbClient.class);

	private Connection connection;

	public DbClient(final String address, final String login, final String password) {

		try {
			Class.forName("org.postgresql.Driver");

			this.connection =
					StringUtils.isNoneBlank(login, password) ? DriverManager.getConnection(address, login, password) : DriverManager.getConnection(address);
			this.connection.setAutoCommit(false);
		} catch (final Exception e) {
			log.error("Connection to postgresDB failed");
			throw new RuntimeException("Connection to postgresDB failed", e);
		}
		log.info("Opened database successfully");
	}

	public void processResults(final String sql, final Consumer<ResultSet> consumer) {

		try (final Statement stmt = connection.createStatement()) {
			stmt.setFetchSize(100);

			try (final ResultSet rs = stmt.executeQuery(sql)) {
				while (rs.next()) {
					consumer.accept(rs);
				}
			} catch (final SQLException e) {
				log.error("Error executing sql query: " + sql, e);
				throw new RuntimeException("Error executing sql query", e);
			}
		} catch (final SQLException e1) {
			log.error("Error preparing sql statement", e1);
			throw new RuntimeException("Error preparing sql statement", e1);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			connection.close();
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

}
