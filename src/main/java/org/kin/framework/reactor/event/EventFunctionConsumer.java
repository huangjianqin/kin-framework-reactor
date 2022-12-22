package org.kin.framework.reactor.event;

import org.kin.framework.event.EventBus;
import org.kin.framework.event.EventFunction;
import org.kin.framework.event.EventHandler;
import org.kin.framework.proxy.ProxyInvoker;
import org.kin.framework.utils.ExceptionUtils;

/**
 * 基于{@link EventFunction}注解的方法的{@link EventConsumer}实现类
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
class EventFunctionConsumer <T> implements EventConsumer<T> {
    /** {@link EventFunction} 注解方法代理类 */
    private final ProxyInvoker<?> invoker;
    /** {@link ReactorEventBus} 参数位置, 默认没有 */
    private final int eventBusParamIdx;
    /** 优先级 */
    private final int order;

     EventFunctionConsumer(ProxyInvoker<?> invoker) {
        this(invoker, 0, LOWEST_PRECEDENCE);
    }

     EventFunctionConsumer(ProxyInvoker<?> invoker, int eventBusParamIdx) {
        this(invoker, eventBusParamIdx, LOWEST_PRECEDENCE);
    }

    EventFunctionConsumer(ProxyInvoker<?> invoker, int eventBusParamIdx, int order) {
        this.invoker = invoker;
        this.eventBusParamIdx = eventBusParamIdx;
        this.order = order;
    }

    @Override
    public void consume(ReactorEventBus eventBus, T event) {
        Object[] params;
        if (eventBusParamIdx == 1) {
            params = new Object[]{eventBus, event};
        } else if (eventBusParamIdx == 2) {
            params = new Object[]{event, eventBus};
        } else {
            params = new Object[]{event};
        }
        try {
            invoker.invoke(params);
        } catch (Exception e) {
            ExceptionUtils.throwExt(e);
        }
    }

    @Override
    public int order() {
        return order;
    }

    //getter
    ProxyInvoker<?> getInvoker() {
        return invoker;
    }
}