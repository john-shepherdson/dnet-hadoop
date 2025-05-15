
package eu.dnetlib.dhp.collection.plugin.sftp;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SftpIteratorWithAuthenticationKey extends AbstractSftpIterator {

	private static final Logger log = LoggerFactory.getLogger(SftpIteratorWithAuthenticationKey.class);
	private final String privateKeyPath;

	public SftpIteratorWithAuthenticationKey(final String baseUrl, final int port, final String username,
		final boolean isRecursive,
		final Set<String> extensionsSet,
		final String fromDate, final String privateKeyPath) {

		super(baseUrl, port, username, isRecursive, extensionsSet, fromDate);

		this.privateKeyPath = privateKeyPath;

		connectToSftpServer();
		initializeQueue();
	}

	@Override
	protected Session createSession(final String address, final int port, final String username) throws JSchException {
		log.debug("Authenticating with private key");

		final JSch jsch = new JSch();
		JSch.setConfig("StrictHostKeyChecking", "no");

		jsch.addIdentity(this.privateKeyPath);

		return jsch.getSession(username, address, port);
	}

}
