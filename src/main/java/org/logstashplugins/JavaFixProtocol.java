package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.Filter;
import co.elastic.logstash.api.FilterMatchListener;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import org.apache.commons.lang3.StringUtils;
import quickfix.ConfigError;
import quickfix.DataDictionary;

import java.util.*;
import java.util.stream.Collectors;

// class name must match plugin name
@LogstashPlugin(name = "java_fix_protocol")
public class JavaFixProtocol implements Filter {

    public static final PluginConfigSpec<String> SOURCE_CONFIG = PluginConfigSpec.stringSetting("fix_message", "message");
    public static final PluginConfigSpec<String> DATA_DICTIONARY_PATH = PluginConfigSpec.stringSetting("data_dictionary_path", "/PATH/TO/YOUR/DD");
    private final DataDictionary dataDictionary;

    private String id;
    private String sourceField;

    public JavaFixProtocol(String id, Configuration config, Context context) throws ConfigError {
        // constructors should validate configuration options
        this.id = id;
        this.sourceField = config.get(SOURCE_CONFIG);
        this.dataDictionary = new DataDictionary(config.get(DATA_DICTIONARY_PATH));
    }

    @Override
    public Collection<Event> filter(Collection<Event> events, FilterMatchListener matchListener) {
        for (Event e : events) {
            Object f = e.getField(sourceField);
            if (f instanceof String) {
                List<Integer> fieldList = Arrays.stream(dataDictionary.getOrderedFields()).boxed().collect(Collectors.toList());
                String[] splits = ((String) f).split("\001");
                List<Integer> unknownFields = new ArrayList<>();
                for (String split : splits) {
                    String[] kv = split.split("=");
                    int field = Integer.parseInt(kv[0]);
                    if (fieldList.contains(field)) {
                        e.setField(dataDictionary.getFieldName(field), kv[1]);
                    } else {
                        unknownFields.add(field);
                        e.setField(kv[0], kv[1]);
                    }
                }
                if (unknownFields.size() > 0) {
                    e.setField("unknownFields", unknownFields);
                }
                matchListener.filterMatched(e);
            }
        }
        return events;
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return Arrays.asList(SOURCE_CONFIG, DATA_DICTIONARY_PATH);
    }

    @Override
    public String getId() {
        return this.id;
    }
}
