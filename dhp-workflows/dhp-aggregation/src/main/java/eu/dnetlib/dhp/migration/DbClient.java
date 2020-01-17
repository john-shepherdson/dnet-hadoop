package eu.dnetlib.dhp.migration;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DbClient implements Closeable {

	private static final Log log = LogFactory.getLog(DbClient.class);

	private Connection connection;

	public DbClient(final String address, final String login, final String password) {

		try {
			Class.forName("org.postgresql.Driver");
			this.connection = DriverManager.getConnection(address, login, password);
			this.connection.setAutoCommit(false);
		} catch (final Exception e) {
			log.error(e.getClass().getName() + ": " + e.getMessage());
			throw new RuntimeException(e);
		}
		log.info("Opened database successfully");
	}

	public void processResults(final String sql, final Consumer<ResultSet> consumer) {

		try (final Statement stmt = connection.createStatement()) {
			try (final ResultSet rs = stmt.executeQuery("SELECT * FROM COMPANY;")) {
				while (rs.next()) {
					consumer.accept(rs);
				}
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		} catch (final SQLException e1) {
			throw new RuntimeException(e1);
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
