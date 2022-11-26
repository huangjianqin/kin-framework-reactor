package org.kin.framework.reactor.event;

import org.kin.framework.utils.OrderedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 用于支持一个event, 对应多个event consumer的场景
 * @author huangjianqin
 * @date 2022/11/26
 */
final class MultiEventConsumers<E> implements EventConsumer<E> {
    private static final Logger log = LoggerFactory.getLogger(MultiEventConsumers.class);

    /** {@link EventConsumer}实现类集合 */
    private volatile List<EventConsumer<E>> consumers = new ArrayList<>();

    @SuppressWarnings("unchecked")
    @Override
    public void consume(ReactorEventBus eventBus, E event) {
        for (EventConsumer<E> consumer : consumers) {
            try {
                EventConsumer.consumeEvent((EventConsumer<Object>) consumer, eventBus, event);
            } catch (Exception e) {
                log.error("event consumer consume event '{}' error {}",event, e);
            }
        }
    }

    /**
     * 基于copy on write更新
     */
    synchronized void addConsumer(EventConsumer<E> consumer) {
        List<EventConsumer<E>> consumers = new ArrayList<>(this.consumers.size() + 1);
        consumers.addAll(this.consumers);
        consumers.add(consumer);
        OrderedUtils.sort(consumers);
        this.consumers = consumers;
    }
}
