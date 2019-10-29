package eu.dnetlib.dhp.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ArgumentApplicationParser implements Serializable {

    private final Options options = new Options();
    private final Map<String, String> objectMap = new HashMap<>();

    public ArgumentApplicationParser(final String json_configuration) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        final OptionsParameter[] configuration = mapper.readValue(json_configuration, OptionsParameter[].class);
        createOptionMap(configuration);
    }

    public ArgumentApplicationParser(final OptionsParameter[] configuration) {
        createOptionMap(configuration);
    }

    private void createOptionMap(final OptionsParameter[] configuration) {

        Arrays.stream(configuration).map(conf -> {
            final Option o = new Option(conf.getParamName(), true, conf.getParamDescription());
            o.setLongOpt(conf.getParamLongName());
            o.setRequired(conf.isParamRequired());
            return o;
        }).forEach(options::addOption);

//        HelpFormatter formatter = new HelpFormatter();
//        formatter.printHelp("myapp", null, options, null, true);


    }

    public void parseArgument(final String[] args) throws Exception {
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        Arrays.stream(cmd.getOptions()).forEach(it -> objectMap.put(it.getLongOpt(), it.getValue()));
    }

    public String get(final String key) {
        return objectMap.get(key);
    }

    public Map<String, String> getObjectMap() {
        return objectMap;
    }
}
