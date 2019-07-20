package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.FilterMatchListener;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstash.plugins.ContextImpl;
import quickfix.ConfigError;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class JavaFixProtocolTest {

    @Test
    public void testJavaExampleFilter() throws ConfigError {
        String sourceField = "fix_message";
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("data_dictionary_path", "/Users/bill/github/FIX42.xml");
        configMap.put("fix_message", sourceField);

        Configuration config = new ConfigurationImpl(configMap);
        Context context = new ContextImpl(null, null);
        JavaFixProtocol filter = new JavaFixProtocol("test-id", config, context);

        Event e = new org.logstash.Event();
        TestMatchListener matchListener = new TestMatchListener();
        e.setField(sourceField, "8=FIX.4.2\0019=184\00135=F\00134=2\00149=ANOTHER_INC\00150=DefaultSenderSubID\00152=20150826-23:08:38.094\00156=DUMMY_INC\0011=DefaultAccount\00111=clordid_of_cancel\00141=151012569\00154=1\00155=ITER\00160=20250407-13:14:15\001167=FUT\001200=201512\00110=147\001");
        Collection<Event> results = filter.filter(Collections.singletonList(e), matchListener);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals("ITER", results.toArray(new Event[1])[0].getField("Symbol"));
        Assert.assertEquals(1, matchListener.getMatchCount());
    }
}

class TestMatchListener implements FilterMatchListener {

    private AtomicInteger matchCount = new AtomicInteger(0);

    @Override
    public void filterMatched(Event event) {
        matchCount.incrementAndGet();
    }

    public int getMatchCount() {
        return matchCount.get();
    }
}