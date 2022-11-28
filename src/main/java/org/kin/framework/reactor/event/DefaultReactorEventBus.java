package org.kin.framework.reactor.event;

import com.google.common.base.Preconditions;
import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.event.EventFunction;
import org.kin.framework.event.EventListener;
import org.kin.framework.event.EventMerge;
import org.kin.framework.proxy.MethodDefinition;
import org.kin.framework.proxy.ProxyInvoker;
import org.kin.framework.proxy.Proxys;
import org.kin.framework.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 事件总线 不保证事件注册的实时性
 * 事件类, 目前事件类最好比较native, 也就是不带泛型的, 也最好不是集合类, 数组等等
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
@SuppressWarnings("rawtypes")
public final class DefaultReactorEventBus implements ReactorEventBus {
    private static final Logger log = LoggerFactory.getLogger(DefaultReactorEventBus.class);

    /** event池 */
    private final Sinks.Many<Object> eventSink = Sinks.many().multicast().directBestEffort();
    /** key -> event class, value -> event consumer */
    private final Map<Class<?>, EventConsumer> event2Consumer = new NonBlockingHashMap<>();
    /** 是否使用字节码增强技术 */
    private final boolean isEnhance;
    /** 调度线程 */
    private final Scheduler scheduler;

    private volatile boolean stopped;

    public DefaultReactorEventBus() {
        this(false);
    }

    public DefaultReactorEventBus(boolean isEnhance) {
        this(isEnhance, Schedulers.boundedElastic());
    }

    public DefaultReactorEventBus(boolean isEnhance, ExecutorService executorService) {
        this(isEnhance, Schedulers.fromExecutorService(executorService));
    }

    public DefaultReactorEventBus(boolean isEnhance, ExecutorService executorService, String executorName) {
        this(isEnhance, Schedulers.fromExecutorService(executorService, executorName));
    }

    @SuppressWarnings("unchecked")
    public DefaultReactorEventBus(boolean isEnhance, Scheduler scheduler) {
        this.isEnhance = isEnhance;
        this.scheduler = scheduler;

        eventSink.asFlux()
                .publishOn(scheduler)
                .subscribe(event -> {
                    try {
                        EventConsumer consumer = event2Consumer.get(event.getClass());
                        if (Objects.nonNull(consumer)) {
                            consumer.consume(this, event);
                        } else {
                            log.warn("can not find event consumer for event '{}'", event);
                        }
                    } catch (Exception e) {
                        log.error("event consumer consume event '{}' error {}", event, e);
                    }
                }, t -> log.error("event sink subscribe error, ", t));
    }

    @Override
    public void register(Object obj) {
        Preconditions.checkNotNull(obj, "param must be not null");
        if (isDisposed()) {
            throw new IllegalStateException("event bus is stopped");
        }

        if (obj instanceof EventConsumer) {
            registerEventConsumer((EventConsumer) obj);
        } else {
            parseEventFuncAndRegister(obj);
        }
    }

    /**
     * 从{@link EventConsumer}实现解析出event class并注册
     */
    private void registerEventConsumer(EventConsumer eventConsumer) {
        Class<? extends EventConsumer> eventConsumerImplClass = eventConsumer.getClass();
        Class<?> eventType = (Class<?>) ClassUtils.getSuperInterfacesGenericActualTypes(EventConsumer.class, eventConsumerImplClass).get(0);

        registerEventConsumer(eventType, eventConsumer, eventConsumerImplClass.getAnnotation(EventMerge.class));
    }

    /**
     * 注册{@link EventConsumer}, 该consumer可能支持事件合并
     */
    private void registerEventConsumer(Class<?> eventType, EventConsumer eventConsumer, EventMerge eventMerge) {
        if (Objects.isNull(eventMerge)) {
            //不支持事件合并
            registerEventConsumer(eventType, eventConsumer);
        } else {
            //支持事件合并
            registerEventConsumer(eventType, new MergedEventConsumer(eventType, eventConsumer, eventMerge, this));
        }
    }

    /**
     * 注册event class及其对应的{@link EventConsumer}实现
     */
    @SuppressWarnings("unchecked")
    private void registerEventConsumer(Class<?> eventType, EventConsumer eventConsumer) {
        EventConsumer registered = event2Consumer.get(eventType);
        if (registered == null) {
            //该event目前只有一个consumer
            event2Consumer.put(eventType, eventConsumer);
        } else if (!(registered instanceof MultiEventConsumers)) {
            //该event目前有两个consumer
            MultiEventConsumers multiConsumers = new MultiEventConsumers();
            multiConsumers.addConsumer(registered);
            multiConsumers.addConsumer(eventConsumer);
            event2Consumer.put(eventType, multiConsumers);
        } else {
            //该event目前有>两个consumer
            ((MultiEventConsumers) registered).addConsumer(eventConsumer);
        }
    }

    /**
     * 解析{@link EventFunction}注解方法并进行注册
     */
    private void parseEventFuncAndRegister(Object obj) {
        Class<?> claxx = obj.getClass();

        if (!claxx.isAnnotationPresent(EventListener.class)) {
            throw new IllegalArgumentException(String.format("%s must be annotated with @%s", obj.getClass(), EventListener.class.getSimpleName()));
        }

        //注解在方法
        //在所有  public & 有注解的  方法中寻找一个匹配的方法作为事件处理方法
        for (Method method : claxx.getMethods()) {
            if (!method.isAnnotationPresent(EventFunction.class)) {
                continue;
            }

            Type[] parameterTypes = method.getGenericParameterTypes();
            int paramNum = parameterTypes.length;
            if (paramNum == 0 || paramNum > 2) {
                //只处理一个或两个参数的public方法
                continue;
            }

            //事件类
            Class<?> eventType = null;
            //ReactorEventBus实现类的方法参数位置, 默认没有
            int eventBusParamIdx = 0;

            EventMerge eventMerge = method.getAnnotation(EventMerge.class);
            if (Objects.isNull(eventMerge)) {
                //不支持事件合并, (ReactorEventBus,Event)
                for (int i = 1; i <= parameterTypes.length; i++) {
                    //普通类型
                    Class<?> parameterType = (Class<?>) parameterTypes[i - 1];
                    if (ReactorEventBus.class.isAssignableFrom(parameterType)) {
                        eventBusParamIdx = i;
                    } else {
                        eventType = parameterType;
                    }
                }
            } else {
                //支持事件合并, (ReactorEventBus,Collection<Event>)
                for (int i = 1; i <= parameterTypes.length; i++) {
                    if (parameterTypes[i - 1] instanceof ParameterizedType) {
                        //泛型类型
                        ParameterizedType parameterizedType = (ParameterizedType) parameterTypes[i - 1];
                        Class<?> parameterizedRawType = (Class<?>) parameterizedType.getRawType();
                        if (Collection.class.isAssignableFrom(parameterizedRawType)) {
                            //事件合并, 获取集合的泛型
                            //以实际事件类型来注册事件处理器
                            eventType = (Class<?>) parameterizedType.getActualTypeArguments()[0];
                        } else {
                            throw new IllegalArgumentException("use merged event, param must be collection, but actually " + parameterizedRawType);
                        }
                    } else {
                        //普通类型
                        Class<?> parameterType = (Class<?>) parameterTypes[i - 1];
                        if (ReactorEventBus.class.isAssignableFrom(parameterType)) {
                            eventBusParamIdx = i;
                        }
                    }
                }
            }

            EventFunction eventFunctionAnno = method.getAnnotation(EventFunction.class);

            registerEventFunc(eventType, generateEventFuncMethodInvoker(obj, method),
                    eventBusParamIdx, eventFunctionAnno.order(), eventMerge);
        }
    }

    /**
     * 注册基于{@link EventFunction}注入的事件处理方法
     *
     * @param eventBusParamIdx {@link ReactorEventBus}实现类的方法参数位置, 默认0, 标识没有该参数
     */
    private void registerEventFunc(Class<?> eventType, ProxyInvoker<?> invoker,
                                   int eventBusParamIdx, int order, EventMerge eventMerge) {
        EventFunctionConsumer consumer = new EventFunctionConsumer<>(
                invoker,
                eventBusParamIdx,
                order);
        registerEventConsumer(eventType, consumer, eventMerge);
    }

    /**
     * @return {@link EventFunction} 注解方法代理类
     */
    private ProxyInvoker<?> generateEventFuncMethodInvoker(Object obj, Method method) {
        MethodDefinition<Object> methodDefinition = new MethodDefinition<>(obj, method);
        if (isEnhance) {
            return Proxys.byteBuddy().enhanceMethod(methodDefinition);
        } else {
            return Proxys.reflection().enhanceMethod(methodDefinition);
        }
    }

    @Override
    public void post(Runnable task) {
        scheduler.schedule(task);
    }

    @Override
    public void post(Object event) {
        eventSink.emitNext(event, (signalType, emitResult) -> {
            //重试则事件触发顺序乱了, 会有问题
            log.error("event dropped, signalType={}, emitResult={}, event={}", signalType, emitResult, event);
            return false;
        });
    }

    @Override
    public Disposable schedule(Object event, long delay, TimeUnit unit) {
        return Mono.just(event)
                .delayElement(Duration.ofMillis(unit.toMillis(delay)), scheduler)
                .subscribe(this::post);
    }

    @Override
    public Disposable scheduleAtFixRate(Object event, long initialDelay, long period, TimeUnit unit) {
        return Flux.interval(Duration.ofMillis(unit.toMillis(initialDelay)), Duration.ofMillis(unit.toMillis(period)),
                        scheduler)
                //转换成event
                .map(t -> event)
                .subscribe(this::post);

    }

    @Override
    public void dispose() {
        if (isDisposed()) {
            return;
        }
        stopped = true;
        if (scheduler != Schedulers.boundedElastic()) {
            //非默认
            scheduler.dispose();
        }
    }

    @Override
    public boolean isDisposed() {
        return stopped;
    }

    //getter
    Scheduler getScheduler() {
        return scheduler;
    }
}
