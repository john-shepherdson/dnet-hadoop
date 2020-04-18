package eu.dnetlib.dhp.migration.actions;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;

public class MigrateActionSet {

    private static final Log log = LogFactory.getLog(MigrateActionSet.class);

    private static final String SEPARATOR = "/";
    private static final String TARGET_PATHS = "target_paths";
    private static final String RAWSET_PREFIX = "rawset_";

    private static Boolean DEFAULT_TRANSFORM_ONLY = false;

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                MigrateActionSet.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/migration/migrate_actionsets_parameters.json")));
        parser.parseArgument(args);

        new MigrateActionSet().run(parser);
    }

    private void run(ArgumentApplicationParser parser) throws Exception {

        final String isLookupUrl = parser.get("isLookupUrl");
        final String sourceNN = parser.get("sourceNameNode");
        final String targetNN = parser.get("targetNameNode");
        final String workDir = parser.get("workingDirectory");
        final Integer distcp_num_maps = Integer.parseInt(parser.get("distcp_num_maps"));

        final String distcp_memory_mb = parser.get("distcp_memory_mb");
        final String distcp_task_timeout = parser.get("distcp_task_timeout");

        final String transform_only_s = parser.get("transform_only");

        log.info("transform only param: " + transform_only_s);

        final Boolean transformOnly = Boolean.valueOf(parser.get("transform_only"));

        log.info("transform only: " + transformOnly);

        ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);

        Configuration conf =
                getConfiguration(distcp_task_timeout, distcp_memory_mb, distcp_num_maps);
        FileSystem targetFS = FileSystem.get(conf);

        Configuration sourceConf =
                getConfiguration(distcp_task_timeout, distcp_memory_mb, distcp_num_maps);
        sourceConf.set(FileSystem.FS_DEFAULT_NAME_KEY, sourceNN);
        FileSystem sourceFS = FileSystem.get(sourceConf);

        Properties props = new Properties();

        List<Path> targetPaths = new ArrayList<>();

        final List<Path> sourcePaths = getSourcePaths(sourceNN, isLookUp);
        log.info(
                String.format(
                        "paths to process:\n%s",
                        sourcePaths.stream()
                                .map(p -> p.toString())
                                .collect(Collectors.joining("\n"))));
        for (Path source : sourcePaths) {

            if (!sourceFS.exists(source)) {
                log.warn(String.format("skipping unexisting path: %s", source));
            } else {

                LinkedList<String> pathQ =
                        Lists.newLinkedList(Splitter.on(SEPARATOR).split(source.toUri().getPath()));

                final String rawSet = pathQ.pollLast();
                log.info(String.format("got RAWSET: %s", rawSet));

                if (StringUtils.isNotBlank(rawSet) && rawSet.startsWith(RAWSET_PREFIX)) {

                    final String actionSetDirectory = pathQ.pollLast();

                    final Path targetPath =
                            new Path(
                                    targetNN
                                            + workDir
                                            + SEPARATOR
                                            + actionSetDirectory
                                            + SEPARATOR
                                            + rawSet);

                    log.info(String.format("using TARGET PATH: %s", targetPath));

                    if (!transformOnly) {
                        if (targetFS.exists(targetPath)) {
                            targetFS.delete(targetPath, true);
                        }
                        runDistcp(
                                distcp_num_maps,
                                distcp_memory_mb,
                                distcp_task_timeout,
                                conf,
                                source,
                                targetPath);
                    }

                    targetPaths.add(targetPath);
                }
            }
        }

        props.setProperty(
                TARGET_PATHS,
                targetPaths.stream().map(p -> p.toString()).collect(Collectors.joining(",")));
        File file = new File(System.getProperty("oozie.action.output.properties"));

        try (OutputStream os = new FileOutputStream(file)) {
            props.store(os, "");
        }
        System.out.println(file.getAbsolutePath());
    }

    private void runDistcp(
            Integer distcp_num_maps,
            String distcp_memory_mb,
            String distcp_task_timeout,
            Configuration conf,
            Path source,
            Path targetPath)
            throws Exception {

        final DistCpOptions op = new DistCpOptions(source, targetPath);
        op.setMaxMaps(distcp_num_maps);
        op.preserve(DistCpOptions.FileAttribute.BLOCKSIZE);
        op.preserve(DistCpOptions.FileAttribute.REPLICATION);
        op.preserve(DistCpOptions.FileAttribute.CHECKSUMTYPE);

        int res =
                ToolRunner.run(
                        new DistCp(conf, op),
                        new String[] {
                            "-Dmapred.task.timeout=" + distcp_task_timeout,
                            "-Dmapreduce.map.memory.mb=" + distcp_memory_mb,
                            "-pb",
                            "-m " + distcp_num_maps,
                            source.toString(),
                            targetPath.toString()
                        });

        if (res != 0) {
            throw new RuntimeException(String.format("distcp exited with code %s", res));
        }
    }

    private Configuration getConfiguration(
            String distcp_task_timeout, String distcp_memory_mb, Integer distcp_num_maps) {
        final Configuration conf = new Configuration();
        conf.set("dfs.webhdfs.socket.connect-timeout", distcp_task_timeout);
        conf.set("dfs.webhdfs.socket.read-timeout", distcp_task_timeout);
        conf.set("dfs.http.client.retry.policy.enabled", "true");
        conf.set("mapred.task.timeout", distcp_task_timeout);
        conf.set("mapreduce.map.memory.mb", distcp_memory_mb);
        conf.set("mapred.map.tasks", String.valueOf(distcp_num_maps));
        return conf;
    }

    private List<Path> getSourcePaths(String sourceNN, ISLookUpService isLookUp)
            throws ISLookUpException {
        String XQUERY =
                "distinct-values(\n"
                        + "let $basePath := collection('/db/DRIVER/ServiceResources/ActionManagerServiceResourceType')//SERVICE_PROPERTIES/PROPERTY[@key = 'basePath']/@value/string()\n"
                        + "for $x in collection('/db/DRIVER/ActionManagerSetDSResources/ActionManagerSetDSResourceType') \n"
                        + "let $setDir := $x//SET/@directory/string()\n"
                        + "let $rawSet := $x//RAW_SETS/LATEST/@id/string()\n"
                        + "return concat($basePath, '/', $setDir, '/', $rawSet))";

        log.info(String.format("running xquery:\n%s", XQUERY));
        return isLookUp.quickSearchProfile(XQUERY).stream()
                .map(p -> sourceNN + p)
                .map(Path::new)
                .collect(Collectors.toList());
    }
}
