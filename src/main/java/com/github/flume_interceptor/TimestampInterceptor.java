package com.github.flume_interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author Sena-wu
 * @date 2022/2/24
 */
public class TimestampInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(JsonStrInterceptor.class);
    private final LongAdder interceptCount = new LongAdder();
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        JSONObject body = (JSONObject) JSONObject.parse(new String(event.getBody(), StandardCharsets.UTF_8));
        logger.info(JSONObject.toJSONString(body));
        if (body != null) {
            Map<String, String> headers = event.getHeaders();
            String ts = body.getString("ts");
            if(ts.length() == 10){
                ts = ts + "000";
            }
            headers.put("timestamp", ts);
            interceptCount.increment();
            return event;
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> eventList = new ArrayList<>(events.size());
        for (Event event : events) {
            Optional.ofNullable(intercept(event)).ifPresent(eventList::add);
        }
        return eventList;
    }

    @Override
    public void close() {
        logger.info("interceptCount: {}", interceptCount.sum());
    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }

    private static String toDateStr(long timestamp) {
        final LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.ofHours(8));
        return localDateTime.format(dateTimeFormatter);
    }
}
