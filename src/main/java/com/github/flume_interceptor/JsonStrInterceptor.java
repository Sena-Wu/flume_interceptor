package com.github.flume_interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

/**
 * json字符串拦截, 过滤数据
 * 过滤非json格式字符串以及不包含特定key的json字符串
 * @author Sena-wu
 * @date 2022/2/24
 */
public class JsonStrInterceptor implements Interceptor {
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
            return event;
        }catch (Exception e){
            interceptCount.increment();
            logger.error("can not parse json: {}", body);
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
            return new JsonStrInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
