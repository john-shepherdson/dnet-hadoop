package eu.dnetlib.dhp.collection.plugin.sftp;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SftpIteratorWithPassword extends AbstractSftpIterator {

	private static final Logger log = LoggerFactory.getLogger(SftpIteratorWithPassword.class);

	private final String password;

	public SftpIteratorWithPassword(final String baseUrl, final int port, final String username, final boolean isRecursive, final Set<String> extensionsSet,
			final String fromDate, final String password) {

		super(baseUrl, port, username, isRecursive, extensionsSet, fromDate);

		this.password = password;
	}

	@Override
	protected Session createSession(final String address, final int port, final String username) throws JSchException {
		log.debug("Authenticating with password");

		final JSch jsch = new JSch();
		JSch.setConfig("StrictHostKeyChecking", "no");

		final Session session = jsch.getSession(username, address, port);
		session.setPassword(this.password);

		return session;
	}

}
