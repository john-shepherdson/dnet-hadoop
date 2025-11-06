
package eu.dnetlib.dhp.collection.plugin.sftp;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

/**
 * Created by andrea on 11/01/16.
 */
public abstract class AbstractSftpIterator implements Iterator<String>, Closeable {

	private static final Logger log = LoggerFactory.getLogger(AbstractSftpIterator.class);

	private static final String END_MESSAGE = "___END___";
	private static final String FAIL_MESSAGE = "___FAIL___";

	private final String sftpURIScheme;
	private final String sftpServerAddress;
	private final String remoteSftpBasePath;
	private final int port;
	private final String username;
	private final boolean isRecursive;
	private final Set<String> extensionsSet;
	private final boolean incremental;

	private Session sftpSession;
	private ChannelSftp sftpChannel;

	private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();

	private String nextElement = null;

	private LocalDate fromDate = null;
	private final DateTimeFormatter simpleDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	public AbstractSftpIterator(final String baseUrl, final int port, final String username, final boolean isRecursive,
			final Set<String> extensionsSet,
			final String fromDate) {

		this.port = port;
		this.username = username;
		this.isRecursive = isRecursive;
		this.extensionsSet = extensionsSet;
		this.incremental = StringUtils.isNotBlank(fromDate);

		if (this.incremental) {
			// I expect fromDate in the format 'yyyy-MM-dd'. See class
			// eu.dnetlib.msro.workflows.nodes.collect.FindDateRangeForIncrementalHarvestingJobNode .
			this.fromDate = LocalDate.parse(fromDate, this.simpleDateTimeFormatter);
			log.debug("fromDate string: " + fromDate + " -- parsed: " + this.fromDate.toString());
		}

		try {
			final URI sftpServer = new URI(baseUrl);
			this.sftpURIScheme = sftpServer.getScheme();
			this.sftpServerAddress = sftpServer.getHost();
			this.remoteSftpBasePath = sftpServer.getPath();
		} catch (final URISyntaxException e) {
			throw new RuntimeException("Bad syntax in the URL " + baseUrl);
		}
	}

	protected void init() {
		log.info(String
				.format("SFTP collector plugin collecting from %s with recursion = %s, incremental = %s with fromDate=%s", this.remoteSftpBasePath, this.isRecursive, this.incremental, this.fromDate));

		new Thread(() -> {
			try {
				connectToSftpServer();
				listDirectoryRecursive(".");
				this.queue.add(END_MESSAGE);
			} catch (final Throwable e) {
				log.error("Error in SFTP thread", e);
				this.queue.add(FAIL_MESSAGE);
			} finally {
				disconnectFromSftpServer();
			}
		}).start();

		try {
			this.nextElement = this.queue.take();
		} catch (final InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private void connectToSftpServer() {

		try {
			this.sftpSession = createSession(this.sftpServerAddress, this.port, this.username);

			this.sftpSession.connect();

			log.debug("SFTP session connected");
			final Channel channel = this.sftpSession.openChannel(this.sftpURIScheme);
			channel.connect();
			this.sftpChannel = (ChannelSftp) channel;
			final String pwd = this.sftpChannel.pwd();
			log.debug("PWD from server: " + pwd);
			final String fullPath = pwd + this.remoteSftpBasePath;
			this.sftpChannel.cd(fullPath);

			log.debug("PWD from server 2 after 'cd " + fullPath + "' : " + this.sftpChannel.pwd());
			log.info("Connected to SFTP server " + this.sftpServerAddress);
		} catch (final JSchException e) {
			log.error("Unable to connect to remote SFTP server.", e);
			throw new RuntimeException("Unable to connect to remote SFTP server.", e);
		} catch (final SftpException e) {
			log.error("Unable to access the base remote path on the SFTP server.", e);
			throw new RuntimeException("Unable to access the base remote path on the SFTP server.", e);
		}
	}

	protected abstract Session createSession(String address, int port, String username) throws JSchException;

	private void disconnectFromSftpServer() {
		if ((this.sftpChannel != null) && !this.sftpChannel.isClosed()) {
			this.sftpChannel.exit();
		}
		if ((this.sftpSession != null) && this.sftpSession.isConnected()) {
			this.sftpSession.disconnect();
			log.info("Disconnected from SFTP server " + this.sftpServerAddress);
		}
	}

	private void listDirectoryRecursive(final String dir) throws Exception {

		log.debug("DIR TO LIST: " + dir);

		for (final Object o : this.sftpChannel.ls(dir)) {
			final ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) o;

			final String fn = entry.getFilename();
			final SftpATTRS attrs = entry.getAttrs();

			if (".".equals(fn) || "..".equals(fn)) {
				// skip parent directory and directory itself
			} else if (isValidFile(fn, attrs) && (!this.incremental || (this.incremental && isRecent(attrs)))) {
				this.queue.add(getFileContent(dir + "/" + fn));
			} else if (attrs.isDir() && this.isRecursive) {
				listDirectoryRecursive(dir + "/" + fn);
			}
		}
	}

	private boolean isValidFile(final String fn, final SftpATTRS attrs) {
		if (attrs.isDir()) { return false; }

		for (final String ext : this.extensionsSet) {
			if (fn.endsWith(ext)) { return true; }
		}

		return false;
	}

	private boolean isRecent(final SftpATTRS attrs) {
		final int mTime = attrs.getMTime();
		// int times are values reduced by the milliseconds, hence we multiply per 1000L

		final LocalDateTime dt = LocalDateTime
				.ofInstant(Instant.ofEpochMilli(mTime * 1000L), TimeZone.getDefault().toZoneId());

		return dt.isAfter(this.fromDate.atStartOfDay());
	}

	private String getFileContent(final String filePath) throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("Collecing file from SFTP: " + filePath);
		}

		try (final OutputStream baos = new ByteArrayOutputStream()) {
			this.sftpChannel.get(filePath, baos);

			return baos.toString();
		}
	}

	@Override
	public boolean hasNext() {
		if (FAIL_MESSAGE.equals(this.nextElement)) { throw new RuntimeException("Collection failed"); }
		return (!END_MESSAGE.equals(this.nextElement));
	}

	@Override
	public String next() {
		try {
			return this.nextElement;
		} finally {
			try {
				this.nextElement = this.queue.take();
			} catch (final InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		disconnectFromSftpServer();
	}

}
