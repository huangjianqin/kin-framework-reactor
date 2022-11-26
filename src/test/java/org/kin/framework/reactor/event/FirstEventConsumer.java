package org.kin.framework.reactor.event;

/**
 * @author huangjianqin
 * @date 2022/11/26
 */
public class FirstEventConsumer implements EventConsumer<FirstEvent> {
    @Override
    public void consume(ReactorEventBus eventBus, FirstEvent event) {
        System.out.println(Thread.currentThread().getName() + " >>> consume " + event.toString());
    }
}
