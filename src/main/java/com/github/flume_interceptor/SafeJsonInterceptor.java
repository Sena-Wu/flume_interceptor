package com.github.flume_interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author Sena-wu
 * @date 2022/7/21
 */
public class SafeJsonInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(JsonStrInterceptor.class);
    private final LongAdder interceptCount = new LongAdder();

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        try{
            JSONObject jsonObject = JSONObject.parseObject(body);
            // 设置header中的timestamp
            if (jsonObject != null) {
                Map<String, String> headers = event.getHeaders();
                String ts = jsonObject.getString("timestamp");
                headers.put("timestamp", ts);

                JSONObject extra = jsonObject.getJSONObject("extra");
                if(extra != null){
                    jsonObject.remove("extra");
                    jsonObject.putAll(extra);
                }
            }

            event.setBody(jsonObject.toJSONString().getBytes(StandardCharsets.UTF_8));
            return event;
        }catch (Exception e){
            interceptCount.increment();
            logger.info(e.getMessage());
            logger.debug("interceptCount: {}", interceptCount.sum());
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

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new SafeJsonInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
