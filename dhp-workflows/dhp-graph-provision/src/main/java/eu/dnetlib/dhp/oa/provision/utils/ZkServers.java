
package eu.dnetlib.dhp.oa.provision.utils;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class ZkServers {

	private static final Log log = LogFactory.getLog(ZkServers.class);
	public static final String SEPARATOR = "/";

	private List<String> hosts;

	private Optional<String> chroot;

	public static ZkServers newInstance(final String zkUrl) {

		// quorum0:2182,quorum1:2182,quorum2:2182,quorum3:2182,quorum4:2182/solr-dev-openaire
		String urls = zkUrl;
		final Optional<String> chRoot = Optional.of(SEPARATOR + StringUtils.substringAfterLast(zkUrl, SEPARATOR));
		if (StringUtils.isNotBlank(chRoot.get())) {
			log.debug(String.format("found zk chroot %s", chRoot));
			urls = zkUrl.replace(chRoot.get(), "");
		}

		final List<String> urlList = Lists.newArrayList(Splitter.on(",").omitEmptyStrings().split(urls));
		log.debug(String.format("zk urls %s", zkUrl));

		return new ZkServers(urlList, chRoot);
	}

	public ZkServers(List<String> hosts, Optional<String> chroot) {
		this.hosts = hosts;
		this.chroot = chroot;
	}

	public List<String> getHosts() {
		return hosts;
	}

	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
	}

	public Optional<String> getChroot() {
		return chroot;
	}

	public void setChroot(Optional<String> chroot) {
		this.chroot = chroot;
	}
}
