
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

	private static final int MAX_RETRIES = 5;
	private static final long BACKOFF_MILLIS = 10000;

	private static final String END_MESSAGE = "___END___";

	private String sftpURIScheme;
	private String sftpServerAddress;
	private String remoteSftpBasePath;
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

	private static String EMPTY_RECORD = "<record/>";

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

	protected void init() {
		log.info(String
				.format("SFTP collector plugin collecting from %s with recursion = %s, incremental = %s with fromDate=%s", this.remoteSftpBasePath, this.isRecursive, this.incremental, this.fromDate));

		new Thread(() -> {
			try {
				connectToSftpServer();
				listDirectoryRecursive(".", "");
				this.queue.add(END_MESSAGE);
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

	private void listDirectoryRecursive(final String parentDir, final String currentDir) {
		String dirToList = parentDir;
		if (StringUtils.isNotBlank(currentDir)) {
			dirToList += "/" + currentDir;
		}
		log.debug("PARENT DIR: " + parentDir);
		log.debug("DIR TO LIST: " + dirToList);
		try {
			for (final Object o : this.sftpChannel.ls(dirToList)) {
				final ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) o;

				String currentFileName = entry.getFilename();
				if (".".equals(currentFileName) || "..".equals(currentFileName)) {
					// skip parent directory and directory itself
					continue;
				}
				final SftpATTRS attrs = entry.getAttrs();
				if (attrs.isDir()) {
					if (this.isRecursive) {
						listDirectoryRecursive(dirToList, currentFileName);
					}
				} else {
					// test the file for extensions compliance and, just in case, add it to the list.
					for (final String ext : this.extensionsSet) {
						if (currentFileName.endsWith(ext)) {
							if (dirToList.length() > 2) {
								currentFileName = dirToList + "/" + currentFileName;
							}
							// test if the file has been changed after the last collection date:
							if (this.incremental) {
								final int mTime = attrs.getMTime();
								// int times are values reduced by the milliseconds, hence we multiply per 1000L
								final LocalDateTime dt = LocalDateTime
										.ofInstant(Instant.ofEpochMilli(mTime * 1000L), TimeZone.getDefault().toZoneId());
								if (dt.isAfter(this.fromDate.atStartOfDay())) {
									this.queue.add(prepareNextElement(currentFileName));
									log.debug(currentFileName + " has changed and must be re-collected");
								} else if (log.isDebugEnabled()) {
									log.debug(currentFileName + " has not changed since last collection");
								}
							} else {
								// if it is not incremental, just add it to the queue
								this.queue.add(prepareNextElement(currentFileName));
							}
						}
					}
				}
			}
		} catch (final SftpException e) {
			throw new RuntimeException("Cannot list the sftp remote directory: " + dirToList, e);
		}
	}

	@Override
	public boolean hasNext() {
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

	private String prepareNextElement(final String remotePath) {

		int nRepeat = 0;

		String fullPathFile = remotePath;
		while (nRepeat < MAX_RETRIES) {
			try {
				final OutputStream baos = new ByteArrayOutputStream();
				this.sftpChannel.get(remotePath, baos);
				if (log.isDebugEnabled()) {
					fullPathFile = this.sftpChannel.pwd() + "/" + remotePath;
					log.debug(String.format("Collected file from SFTP: %s%s", this.sftpServerAddress, fullPathFile));
				}
				final String doc = baos.toString();
				if (StringUtils.isNotBlank(doc)) { return doc; }

				return EMPTY_RECORD;
			} catch (final SftpException e) {
				nRepeat++;
				log
						.warn(String
								.format("An error occurred [%s] for %s%s, retrying.. [retried %s time(s)]", e
										.getMessage(), this.sftpServerAddress, fullPathFile, nRepeat));
				// disconnectFromSftpServer();
				try {
					Thread.sleep(BACKOFF_MILLIS);
				} catch (final InterruptedException e1) {
					log.error(e1.getMessage(), e1);
				}
			}
		}
		throw new RuntimeException(
				String
						.format("Impossible to retrieve FTP file %s after %s retries. Aborting FTP collection.", fullPathFile, nRepeat));
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
