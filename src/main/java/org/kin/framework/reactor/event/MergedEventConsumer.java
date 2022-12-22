package org.kin.framework.reactor.event;

import org.kin.framework.event.EventMerge;
import org.kin.framework.event.MergeType;
import org.kin.framework.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Objects;

/**
 * 支持事件合并的{@link EventConsumer}实现
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
@SuppressWarnings("rawtypes")
class MergedEventConsumer<E> implements EventConsumer<E> {
    private static final Logger log = LoggerFactory.getLogger(MergedEventConsumer.class);

    /** 事件类型 */
    private final Class<?> eventType;
    /** 委托的event consumer */
    private final EventConsumer delegate;
    /** 事件合并参数 */
    private final EventMerge eventMerge;
    /** 绑定的{@link DefaultReactorEventBus} */
    private final DefaultReactorEventBus eventBus;
    /** event池 */
    private final Sinks.Many<Object> eventSink = Sinks.many().replay().all();

    MergedEventConsumer(Class<?> eventType, EventConsumer delegate, EventMerge eventMerge, DefaultReactorEventBus eventBus) {
        this.eventType = eventType;
        this.delegate = delegate;
        this.eventMerge = eventMerge;
        this.eventBus = eventBus;
        mergeEvent();
    }

    @Override
    public void consume(ReactorEventBus eventBus, E event) {
        if (!Objects.equals(this.eventBus, eventBus)) {
            throw new IllegalStateException("event bus is illegal");
        }

        eventSink.emitNext(event, (signalType, emitResult) -> {
            //重试则事件触发顺序乱了, 会有问题
            log.error("event dropped, signalType={}, emitResult={}, event={}", signalType, emitResult, event);
            return false;
        });
    }

    /**
     * 事件合并
     */
    private void mergeEvent() {
        MergeType type = eventMerge.type();
        if (MergeType.WINDOW.equals(type)) {
            mergeWindowEvent();
        } else if (MergeType.DEBOUNCE.equals(type)) {
            mergeDebounceEvent();
        } else {
            throw new UnsupportedOperationException(String.format("doesn't support merge type '%s'", type));
        }
    }

    /**
     * 根据窗口规则, 合并事件
     */
    @SuppressWarnings("unchecked")
    private void mergeWindowEvent() {
        //启动window
        eventSink.asFlux()
                .buffer(Duration.ofMillis(eventMerge.unit().toMillis(eventMerge.window())), eventBus.getScheduler())
                .subscribe(events -> {
                    try {
                        if (CollectionUtils.isNonEmpty(events)) {
                            EventConsumer.consumeEvent(delegate, eventBus, events);
                        }
                    } catch (Exception e) {
                        log.error("event consumer consume merge event '{}' error {}", eventType, e);
                    }

                }, t -> log.error("event sink subscribe error, ", t));
    }

    /**
     * 根据抖动规则, 合并事件
     */
    @SuppressWarnings("unchecked")
    private void mergeDebounceEvent() {
        //启动debounce
        eventSink.asFlux()
                .bufferTimeout(eventMerge.maxSize(), Duration.ofMillis(eventMerge.unit().toMillis(eventMerge.window())),
                        eventBus.getScheduler())
                .subscribe(events -> {
                    try {
                        if (CollectionUtils.isNonEmpty(events)) {
                            EventConsumer.consumeEvent(delegate, eventBus, events);
                        }
                    } catch (Exception e) {
                        log.error("event consumer consume merge event '{}' error {}", eventType, e);
                    }
                }, t -> log.error("event sink subscribe error, ", t));
    }
}
