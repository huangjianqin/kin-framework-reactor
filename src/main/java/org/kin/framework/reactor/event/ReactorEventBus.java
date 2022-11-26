package org.kin.framework.reactor.event;

import reactor.core.Disposable;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 事件总线接口
 * @author huangjianqin
 * @date 2022/11/26
 */
public interface ReactorEventBus extends Disposable {
    /**
     * 注册{@link EventConsumer}
     * @param obj  {@link EventConsumer}实现类或者public方法带有{@link org.kin.framework.event.EventFunction} 注解的实例
     */
    void register(Object obj);

    /**
     * 直接执行task
     *
     * @param task 任务逻辑
     */
    void post(Runnable task);

    /**
     * 分发事件
     *
     * @param event 事件实例
     */
    void post(Object event);

    /**
     * 延迟调度事件分发
     *
     * @param event 事件
     * @param delay  延迟执行时间
     * @param unit  时间单位
     */
    Disposable schedule(Object event, long delay, TimeUnit unit);

    /**
     * 固定时间间隔调度事件分发
     *
     * @param event        事件
     * @param initialDelay 延迟执行时间
     * @param period       时间间隔
     * @param unit         时间单位
     */
    Disposable scheduleAtFixRate(Object event, long initialDelay, long period, TimeUnit unit);

}
