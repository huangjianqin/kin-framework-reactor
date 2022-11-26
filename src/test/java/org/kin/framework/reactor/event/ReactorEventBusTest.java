package org.kin.framework.reactor.event;

import org.kin.framework.common.Ordered;
import org.kin.framework.event.DefaultEventBus;
import org.kin.framework.event.EventFunction;
import org.kin.framework.event.EventMerge;
import org.kin.framework.event.MergeType;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author huangjianqin
 * @date 2022/11/26
 */
public class ReactorEventBusTest {
    public static void main(String[] args) throws InterruptedException {
        ReactorEventBus eventBus = new DefaultReactorEventBus();
        eventBus.register(new ReactorEventBusTest());
        eventBus.register(new FirstEventConsumer());

        eventBus.post(new FirstEvent());
        eventBus.post(new SecondEvent());

        System.out.println(System.currentTimeMillis() + " >>> post ThirdEvent to event bus");
        for (int i = 0; i < 8; i++) {
            eventBus.post(new ThirdEvent());
            Thread.sleep(200);
        }
        System.out.println(System.currentTimeMillis() + " >>> finish post ThirdEvent to event bus");

        Thread.sleep(5_000);
        System.out.println("reactor event bus prepare to shutdown");
        eventBus.dispose();
    }

    @EventFunction(order = Ordered.HIGHEST_PRECEDENCE)
    public void consumeFirstEvent1(FirstEvent event) {
        System.out.println("1 >>> consume " + event.toString());
    }

    @EventFunction
    public void consumeFirstEvent2(FirstEvent event) {
        System.out.println("2 >>> consume " + event.toString());
    }

    @EventFunction
    public void consumeSecondEvent(SecondEvent event) {
        System.out.println("consume " + event.toString());
    }

    @EventMerge(window = 1000)
    @EventFunction
    public void consumeThirdEvent1(List<ThirdEvent> events) {
        System.out.println(Thread.currentThread().getName() + "-" +System.currentTimeMillis() + ": consumeThirdEvent1 >>> consume " + events);
    }

    @EventMerge(type = MergeType.DEBOUNCE, window = 1000, maxSize = 3)
    @EventFunction
    public void consumeThirdEvent2(List<ThirdEvent> events) {
        System.out.println(Thread.currentThread().getName() + "-" +System.currentTimeMillis() + ": consumeThirdEvent2 >>> consume " + events);
    }
}
