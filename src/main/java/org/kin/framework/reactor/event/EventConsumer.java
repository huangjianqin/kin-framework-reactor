package org.kin.framework.reactor.event;

import org.kin.framework.common.Ordered;
import org.kin.framework.event.EventHandler;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.EventListener;
import java.util.Objects;

/**
 * 事件消费者
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
public interface EventConsumer<E> extends EventListener, Ordered {
    /**
     * 事件消费逻辑实现
     *
     * @param eventBus 所属{@link ReactorEventBus}
     * @param event    事件
     */
    void consume(ReactorEventBus eventBus, E event);

    /**
     * @return 事件消息所在的 {@link Scheduler}
     */
    default Scheduler scheduler() {
        return null;
    }

    /**
     * {@link EventConsumer}处理event统一处理逻辑
     *
     * @param consumer event consumer impl
     * @param eventBus 所属event bus
     * @param event    实际event
     */
    static void consumeEvent(EventConsumer<Object> consumer, ReactorEventBus eventBus, Object event) {
        Scheduler scheduler = consumer.scheduler();
        if (Objects.nonNull(scheduler)) {
            Mono.just(event)
                    .subscribeOn(scheduler)
                    .subscribe(e -> consumer.consume(eventBus, event));
        } else {
            consumer.consume(eventBus, event);
        }
    }
}
