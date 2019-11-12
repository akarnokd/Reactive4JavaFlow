/*
 * Copyright 2017 David Karnok
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hu.akarnokd.reactive4javaflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Stream;

import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.impl.FunctionalHelper;
import hu.akarnokd.reactive4javaflow.impl.consumers.*;
import hu.akarnokd.reactive4javaflow.impl.operators.*;
import hu.akarnokd.reactive4javaflow.impl.schedulers.ImmediateSchedulerService;

public abstract class Esetleg<T> implements FolyamPublisher<T> {

    @SuppressWarnings("unchecked")
    @Override
    public final void subscribe(Flow.Subscriber<? super T> s) {
        Objects.requireNonNull(s, "s == null");
        if (s instanceof FolyamSubscriber) {
            subscribe((FolyamSubscriber<? super T>)s);
        } else {
            subscribe(new StrictSubscriber<>(s));
        }
    }

    @Override
    public final void subscribe(FolyamSubscriber<? super T> s) {
        s = Objects.requireNonNull(FolyamPlugins.onSubscribe(this, s), "The plugin onSubscribe handler returned a null value");
        try {
            subscribeActual(s);
        } catch (Throwable ex) {
            FolyamPlugins.onError(ex);
        }
    }

    protected abstract void subscribeActual(FolyamSubscriber<? super T> s);

    public final <R> R to(Function<? super Esetleg<T>, R> converter) {
        return converter.apply(this);
    }

    public final <R> Esetleg<R> compose(EsetlegTransformer<T, R> composer) {
        return composer.apply(this);
    }

    public final AutoDisposable subscribe() {
        return subscribe(v -> { }, FolyamPlugins::onError, () -> { });
    }

    public final AutoDisposable subscribe(CheckedConsumer<? super T> onNext) {
        return subscribe(onNext, FolyamPlugins::onError, () -> { });
    }

    public final AutoDisposable subscribe(CheckedConsumer<? super T> onNext, CheckedConsumer<? super Throwable> onError) {
        return subscribe(onNext, onError, () -> { });
    }

    public final AutoDisposable subscribe(CheckedConsumer<? super T> onNext, CheckedConsumer<? super Throwable> onError, CheckedRunnable onComplete) {
        LambdaConsumer<T> consumer = new LambdaConsumer<>(onNext, onError, onComplete, FunctionalHelper.REQUEST_UNBOUNDED);
        subscribe(consumer);
        return consumer;
    }

    public final TestConsumer<T> test() {
        TestConsumer<T> tc = new TestConsumer<>();
        subscribe(tc);
        return tc;
    }

    public final TestConsumer<T> test(long initialRequest) {
        TestConsumer<T> tc = new TestConsumer<>(initialRequest);
        subscribe(tc);
        return tc;
    }

    public final TestConsumer<T> test(long initialRequest, boolean cancelled, int fusionMode) {
        TestConsumer<T> tc = new TestConsumer<>(initialRequest);
        if (cancelled) {
            tc.close();
        }
        tc.requestFusionMode(fusionMode);
        subscribe(tc);
        return tc;
    }

    @SuppressWarnings("unchecked")
    public final void safeSubscribe(Flow.Subscriber<? super T> s) {
        Objects.requireNonNull(s, "s == null");
        if (s instanceof FolyamSubscriber) {
            subscribe((FolyamSubscriber<? super T>)s);
        } else {
            subscribe(new SafeFolyamSubscriber<>(new StrictSubscriber<>(s)));
        }
    }

    public final <E extends Flow.Subscriber<? super T>> E subscribeWith(E s) {
        subscribe(s);
        return s;
    }

    // -----------------------------------------------------------------------------------
    // Source operators
    // -----------------------------------------------------------------------------------

    public static <T> Esetleg<T> just(T item) {
        Objects.requireNonNull(item, "item == null");
        return FolyamPlugins.onAssembly(new EsetlegJust<>(item));
    }

    @SuppressWarnings("unchecked")
    public static <T> Esetleg<T> empty() {
        return FolyamPlugins.onAssembly((Esetleg<T>) EsetlegEmpty.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    public static <T> Esetleg<T> never() {
        return FolyamPlugins.onAssembly((Esetleg<T>) EsetlegNever.INSTANCE);
    }

    public static <T> Esetleg<T> error(Throwable error) {
        Objects.requireNonNull(error, "error == null");
        return FolyamPlugins.onAssembly(new EsetlegError<>(error));
    }

    public static <T> Esetleg<T> error(Callable<? extends Throwable> errorSupplier) {
        Objects.requireNonNull(errorSupplier, "errorSupplier == null");
        return FolyamPlugins.onAssembly(new EsetlegErrorCallable<>(errorSupplier));
    }

    public static <T> Esetleg<T> create(CheckedConsumer<? super FolyamEmitter<T>> onSubscribe) {
        Objects.requireNonNull(onSubscribe, "onSubscribe == null");
        return FolyamPlugins.onAssembly(new EsetlegCreate<>(onSubscribe));
    }

    public static <T> Esetleg<T> fromCallable(Callable<? extends T> call) {
        Objects.requireNonNull(call, "call == null");
        return FolyamPlugins.onAssembly(new EsetlegCallable<>(call));
    }

    public static <T> Esetleg<T> fromCallableAllowEmpty(Callable<? extends T> call) {
        Objects.requireNonNull(call, "call == null");
        return FolyamPlugins.onAssembly(new EsetlegCallableAllowEmpty<>(call));
    }

    public static <T> Esetleg<T> fromCompletionStage(CompletionStage<? extends T> stage) {
        Objects.requireNonNull(stage, "stage == null");
        return FolyamPlugins.onAssembly(new EsetlegCompletionStage<>(stage));
    }

    public static <T> Esetleg<T> fromFuture(Future<? extends T> future) {
        Objects.requireNonNull(future, "future == null");
        return FolyamPlugins.onAssembly(new EsetlegFuture<>(future, 0L, null));
    }

    public static <T> Esetleg<T> fromFuture(Future<? extends T> future, long timeout, TimeUnit unit) {
        Objects.requireNonNull(future, "future == null");
        Objects.requireNonNull(unit, "unit == null");
        return FolyamPlugins.onAssembly(new EsetlegFuture<>(future, timeout, unit));
    }

    public static <T> Esetleg<T> fromOptional(Optional<? extends T> optional) {
        Objects.requireNonNull(optional, "optional == null");
        return optional.isPresent() ? just(optional.get()) : empty();
    }

    @SuppressWarnings("unchecked")
    public static <T> Esetleg<T> fromPublisher(Flow.Publisher<? extends T> source) {
        Objects.requireNonNull(source, "source == null");
        if (source instanceof Esetleg) {
            return (Esetleg<T>)source;
        }
        return FolyamPlugins.onAssembly(new EsetlegWrap<>(source));
    }

    public static Esetleg<Long> timer(long delay, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new EsetlegTimer(delay, unit, executor));
    }

    public static <T> Esetleg<T> defer(Callable<? extends Esetleg<T>> esetlegFactory) {
        Objects.requireNonNull(esetlegFactory, "esetlegFactory == null");
        return FolyamPlugins.onAssembly(new EsetlegDefer<>(esetlegFactory));
    }

    public static <T, R> Esetleg<T> using(Callable<R> resourceSupplier, CheckedFunction<? super R, ? extends Esetleg<? extends T>> flowSupplier, CheckedConsumer<? super R> resourceCleaner) {
        return using(resourceSupplier, flowSupplier, resourceCleaner, false);
    }

    public static <T, R> Esetleg<T> using(Callable<R> resourceSupplier, CheckedFunction<? super R, ? extends Esetleg<? extends T>> flowSupplier, CheckedConsumer<? super R> resourceCleaner, boolean eagerCleanup) {
        Objects.requireNonNull(resourceSupplier, "resourceSupplier == null");
        Objects.requireNonNull(flowSupplier, "flowSupplier == null");
        Objects.requireNonNull(resourceCleaner, "resourceCleaner == null");
        return FolyamPlugins.onAssembly(new EsetlegUsing<>(resourceSupplier, flowSupplier, resourceCleaner, eagerCleanup));
    }

    // -----------------------------------------------------------------------------------
    // Static combinator operators
    // -----------------------------------------------------------------------------------

    public static <T> Esetleg<T> amb(Iterable<? extends Esetleg<? extends T>> sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new EsetlegAmbIterable<>(sources));
    }

    @SafeVarargs
    public static <T> Esetleg<T> ambArray(Esetleg<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new EsetlegAmbArray<>(sources));
    }

    public static <T, R> Esetleg<R> zip(Iterable<? extends Esetleg<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new EsetlegZipIterable<>(sources, zipper, false));
    }

    public static <T, R> Esetleg<R> zipDelayError(Iterable<? extends Esetleg<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new EsetlegZipIterable<>(sources, zipper, true));
    }

    @SafeVarargs
    public static <T, R> Esetleg<R> zipArray(CheckedFunction<? super Object[], ? extends R> zipper, Esetleg<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new EsetlegZipArray<>(sources, zipper, false));
    }

    @SafeVarargs
    public static <T, R> Esetleg<R> zipArrayDelayError(CheckedFunction<? super Object[], ? extends R> zipper, Esetleg<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new EsetlegZipArray<>(sources, zipper, true));
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> Esetleg<R> zip(Esetleg<T> source1, Esetleg<U> source2, CheckedBiFunction<? super T, ? super U, ? extends R> zipper) {
        Objects.requireNonNull(source1, "sources1 == null");
        Objects.requireNonNull(source2, "sources2 == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new EsetlegZipArray<T, R>(new Esetleg[] { source1, source2 }, a -> zipper.apply((T)a[0], (U)a[1]), false));
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> Esetleg<R> zipDelayError(Esetleg<T> source1, Esetleg<U> source2, CheckedBiFunction<? super T, ? super U, ? extends R> zipper) {
        Objects.requireNonNull(source1, "sources1 == null");
        Objects.requireNonNull(source2, "sources2 == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new EsetlegZipArray<T, R>(new Esetleg[] { source1, source2 }, a -> zipper.apply((T)a[0], (U)a[1]), true));
    }

    public static <T> Esetleg<Boolean> sequenceEqual(Flow.Publisher<? extends T> first, Flow.Publisher<? extends T> second) {
        return sequenceEqual(first, second, Objects::equals);
    }

    public static <T> Esetleg<Boolean> sequenceEqual(Flow.Publisher<? extends T> first, Flow.Publisher<? extends T> second, int prefetch) {
        return sequenceEqual(first, second, Objects::equals, prefetch);
    }

    public static <T> Esetleg<Boolean> sequenceEqual(Flow.Publisher<? extends T> first, Flow.Publisher<? extends T> second, CheckedBiPredicate<? super T, ? super T> isEqual) {
        return sequenceEqual(first, second, isEqual, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Esetleg<Boolean> sequenceEqual(Flow.Publisher<? extends T> first, Flow.Publisher<? extends T> second, CheckedBiPredicate<? super T, ? super T> isEqual, int prefetch) {
        Objects.requireNonNull(first, "first == null");
        Objects.requireNonNull(second, "second == null");
        Objects.requireNonNull(isEqual, "isEqual == null");
        return FolyamPlugins.onAssembly(new EsetlegSequenceEqual<>(first, second, isEqual, prefetch));
    }

    // -----------------------------------------------------------------------------------
    // Instance operators
    // -----------------------------------------------------------------------------------

    public final <R> Esetleg<R> map(CheckedFunction<? super T, ? extends R> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new EsetlegMap<>(this, mapper));
    }

    public final <R> Esetleg<R> mapOptional(CheckedFunction<? super T, ? extends Optional<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new EsetlegMapOptional<>(this, mapper));
    }

    public final <R> Esetleg<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return mapWhen(mapper, (a, b) -> b);
    }

    public final <U, R> Esetleg<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
        Objects.requireNonNull(mapper, "mapper == null");
        Objects.requireNonNull(combiner, "combiner == null");
        return FolyamPlugins.onAssembly(new EsetlegMapWhen<>(this, mapper, combiner, false));
    }

    public final Esetleg<T> filter(CheckedPredicate<? super T> filter) {
        Objects.requireNonNull(filter, "filter == null");
        return FolyamPlugins.onAssembly(new EsetlegFilter<>(this, filter));
    }

    public final Esetleg<T> filterWhen(CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter) {
        Objects.requireNonNull(filter, "filter == null");
        return FolyamPlugins.onAssembly(new EsetlegFilterWhen<>(this, filter, false));
    }

    public final Esetleg<T> takeUntil(Flow.Publisher<?> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(new EsetlegTakeUntil<>(this, other));
    }

    public final Esetleg<T> delaySubscription(Flow.Publisher<?> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(new EsetlegDelaySubscription<>(this, other));
    }

    public final Folyam<T> repeat() {
        return repeat(Long.MAX_VALUE, () -> true);
    }

    public final Folyam<T> repeat(long times) {
        return repeat(times, () -> true);
    }

    public final Folyam<T> repeat(CheckedBooleanSupplier condition) {
        return repeat(Long.MAX_VALUE, condition);
    }

    public final Folyam<T> repeat(long times, CheckedBooleanSupplier condition) {
        Objects.requireNonNull(condition, "condition == null");
        return FolyamPlugins.onAssembly(new FolyamRepeat<>(this, times, condition));
    }

    public final Folyam<T> repeatWhen(CheckedFunction<? super Folyam<Object>, ? extends Flow.Publisher<?>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new FolyamRepeatWhen<>(this, handler));
    }

    public final Esetleg<T> switchIfEmpty(Esetleg<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(new EsetlegSwitchIfEmpty<>(this, other));
    }

    public final Esetleg<T> switchIfEmptyMany(Iterable<? extends Esetleg<? extends T>> others) {
        Objects.requireNonNull(others, "others == null");
        return FolyamPlugins.onAssembly(new EsetlegSwitchIfEmptyMany<>(this, others));
    }

    public final Esetleg<T> defaultIfEmpty(T item) {
        return switchIfEmpty(Esetleg.just(item));
    }

    public final Esetleg<T> onTerminateDetach() {
        return FolyamPlugins.onAssembly(new EsetlegOnTerminateDetach<>(this));
    }

    public final Esetleg<T> ignoreElement() {
        return FolyamPlugins.onAssembly(new EsetlegIgnoreElement<>(this));
    }

    public final Esetleg<Boolean> equalsWith(Flow.Publisher<? extends T> other) {
        return Esetleg.sequenceEqual(this, other);
    }

    public final Esetleg<T> hide() {
        return FolyamPlugins.onAssembly(new EsetlegHide<>(this));
    }

    public final Folyam<T> toFolyam() {
        return FolyamPlugins.onAssembly(new FolyamWrap<>(this));
    }

    // mappers of inner flows

    public final <R> Esetleg<R> flatMap(CheckedFunction<? super T, ? extends Esetleg<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new EsetlegFlatMap<>(this, mapper));
    }

    public final <R> Folyam<R> flatMapPublisher(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new EsetlegFlatMapPublisher<>(this, mapper));
    }

    public final <R> Folyam<R> flatMapIterable(CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new EsetlegFlatMapIterable<>(this, mapper));
    }

    public final <R> Folyam<R> flatMapStream(CheckedFunction<? super T, ? extends Stream<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new EsetlegFlatMapStream<>(this, mapper));
    }

    // async-introducing operators

    public final Esetleg<T> subscribeOn(SchedulerService executor) {
        return subscribeOn(executor, !(this instanceof EsetlegCreate));
    }

    public final Esetleg<T> subscribeOn(SchedulerService executor, boolean requestOn) {
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new EsetlegSubscribeOn<>(this, executor, requestOn));
    }

    public final Esetleg<T> observeOn(SchedulerService executor) {
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new EsetlegObserveOn<>(this, executor));
    }

    public final Esetleg<T> delay(long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new EsetlegDelayTime<>(this, time, unit, executor));
    }

    public final Esetleg<T> delay(CheckedFunction<? super T, ? extends Flow.Publisher<?>> delaySelector) {
        Objects.requireNonNull(delaySelector, "delaySelector == null");
        return FolyamPlugins.onAssembly(new EsetlegDelaySelector<>(this, delaySelector));
    }

    // state-peeking operators

    public final Esetleg<T> doOnSubscribe(CheckedConsumer<? super Flow.Subscription> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(EsetlegDoOnSignal.withOnSubscribe(this, handler));
    }

    public final Esetleg<T> doOnNext(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(EsetlegDoOnSignal.withOnNext(this, handler));
    }

    public final Esetleg<T> doAfterNext(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(EsetlegDoOnSignal.withOnAfterNext(this, handler));
    }

    public final Esetleg<T> doOnError(CheckedConsumer<? super Throwable> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(EsetlegDoOnSignal.withOnError(this, handler));
    }

    public final Esetleg<T> doOnComplete(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(EsetlegDoOnSignal.withOnComplete(this, handler));
    }

    public final Esetleg<T> doFinally(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        return doFinally(handler, ImmediateSchedulerService.INSTANCE);
    }

    public final Esetleg<T> doFinally(CheckedRunnable handler, SchedulerService executor) {
        Objects.requireNonNull(handler, "handler == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new EsetlegDoFinally<>(this, handler, executor));
    }

    public final Esetleg<T> doOnRequest(CheckedConsumer<? super Long> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(EsetlegDoOnSignal.withOnRequest(this, handler));
    }

    public final Esetleg<T> doOnCancel(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(EsetlegDoOnSignal.withOnCancel(this, handler));
    }

    // resilience operators

    public final Esetleg<T> timeout(long timeout, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new EsetlegTimeoutTimed<>(this, timeout, unit, executor));
    }

    public final Esetleg<T> timeout(long timeout, TimeUnit unit, SchedulerService executor, Esetleg<? extends T> fallback) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        Objects.requireNonNull(fallback, "fallback == null");
        return FolyamPlugins.onAssembly(new EsetlegTimeoutTimedFallback<>(this, timeout, unit, executor, fallback));
    }

    public final Esetleg<T> timeout(Flow.Publisher<?> firstTimeout) {
        Objects.requireNonNull(firstTimeout, "firstTimeout == null");
        return FolyamPlugins.onAssembly(new EsetlegTimeoutSelector<>(this, firstTimeout));
    }

    public final Esetleg<T> timeout(Flow.Publisher<?> firstTimeout, Esetleg<? extends T> fallback) {
        Objects.requireNonNull(firstTimeout, "firstTimeout == null");
        Objects.requireNonNull(fallback, "fallback == null");
        return FolyamPlugins.onAssembly(new EsetlegTimeoutSelectorFallback<>(this, firstTimeout, fallback));
    }

    public final Esetleg<T> onErrorComplete() {
        return FolyamPlugins.onAssembly(new EsetlegOnErrorComplete<>(this));
    }

    public final Esetleg<T> onErrorReturn(T item) {
        Objects.requireNonNull(item, "item == null");
        return onErrorFallback(Esetleg.just(item));
    }

    public final Esetleg<T> onErrorFallback(Esetleg<? extends T> fallback) {
        Objects.requireNonNull(fallback, "fallback == null");
        return onErrorResumeNext(e -> fallback);
    }

    public final Esetleg<T> onErrorResumeNext(CheckedFunction<? super Throwable, ? extends Esetleg<? extends T>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new EsetlegOnErrorResumeNext<>(this, handler));
    }

    public final Esetleg<T> retry() {
        return retry(Long.MAX_VALUE, e -> true);
    }

    public final Esetleg<T> retry(long times) {
        return retry(times, e -> true);
    }

    public final Esetleg<T> retry(CheckedPredicate<? super Throwable> condition) {
        return retry(Long.MAX_VALUE, condition);
    }

    public final Esetleg<T> retry(long times, CheckedPredicate<? super Throwable> condition) {
        Objects.requireNonNull(condition, "condition == null");
        return FolyamPlugins.onAssembly(new EsetlegRetry<>(this, times, condition));
    }

    public final Esetleg<T> retryWhen(CheckedFunction<? super Folyam<Throwable>, ? extends Flow.Publisher<?>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new EsetlegRetryWhen<>(this, handler));
    }
    // pair combinators

    public final Folyam<T> startWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return Folyam.concatArray(other, this);
    }

    public final Esetleg<T> ambWith(Esetleg<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return EsetlegAmbArray.ambWith(this, other);
    }

    public final Folyam<T> concatWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return Folyam.concatArray(this, other);
    }

    public final Folyam<T> mergeWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return Folyam.mergeArray(this, other);
    }

    @SuppressWarnings("unchecked")
    public final <U, R> Esetleg<R> zipWith(Esetleg<? extends T> other, CheckedBiFunction<? super T, ? super U, ? extends R> zipper) {
        Objects.requireNonNull(other, "other == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return zipArray(a -> zipper.apply((T)a[0], (U)a[1]), this, other);
    }

    /**
     * Consumes the current Esetleg, ignoring its potential value, then
     * subscribes to the next Esetleg and relays its event(s).
     * @param <U> the next Esetleg's value type
     * @param next the next Esetleg to consume after this completes
     * @return the new Esetleg instance
     * @since 0.1.2
     */
    public final <U> Esetleg<U> andThen(Esetleg<U> next) {
        Objects.requireNonNull(next, "next == null");
        return FolyamPlugins.onAssembly(new EsetlegAndThen<>(this, next));
    }

    /**
     * Consumes the current Esetleg, ignoring its potential value, then
     * subscribes to the next Flow.Publisher and relays its event(s).
     * @param <U> the next Esetleg's value type
     * @param next the next Flow.Publisher to consume after this completes
     * @return the new Folyam instance
     * @since 0.1.2
     */
    public final <U> Folyam<U> andThen(Flow.Publisher<U> next) {
        Objects.requireNonNull(next, "next == null");
        return FolyamPlugins.onAssembly(new FolyamAndThen<>(this, next));
    }

    // cold-processors conversion operators

    public final ConnectableFolyam<T> publish() {
        return FolyamPlugins.onAssembly(new ConnectableFolyamPublish<>(this, 1));
    }

    public final <R> Esetleg<R> publish(CheckedFunction<? super Folyam<T>, ? extends Esetleg<? extends R>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new EsetlegPublish<>(this, handler));
    }

    public final Esetleg<T> cache() {
        return FolyamPlugins.onAssembly(new EsetlegForcedWrap<>(replay().autoConnect()));
    }

    public final ConnectableFolyam<T> replay() {
        return FolyamPlugins.onAssembly(new ConnectableFolyamReplayUnbounded<>(this, 1));
    }

    public final <R> Esetleg<R> replay(CheckedFunction<? super Folyam<T>, ? extends Esetleg<? extends R>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new EsetlegReplay<>(this, handler));
    }

    public final <U, R> Esetleg<R> multicast(CheckedFunction<? super Esetleg<T>, ? extends ConnectableFolyam<U>> multicaster, CheckedFunction<? super Folyam<U>, ? extends Esetleg<? extends R>> handler) {
        Objects.requireNonNull(multicaster, "multicaster == null");
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new EsetlegMulticast<>(this, multicaster, handler));
    }

    // -----------------------------------------------------------------------------------
    // Blocking operators
    // -----------------------------------------------------------------------------------

    public final Optional<T> blockingGet() {
        BlockingLastConsumer<T> s = new BlockingLastConsumer<>();
        subscribe(s);
        return Optional.ofNullable(s.blockingGet());
    }

    public final Optional<T> blockingGet(long timeout, TimeUnit unit) {
        BlockingLastConsumer<T> s = new BlockingLastConsumer<>();
        subscribe(s);
        return Optional.ofNullable(s.blockingGet(timeout, unit));
    }

    public final T blockingGet(T defaultItem) {
        BlockingLastConsumer<T> s = new BlockingLastConsumer<>();
        subscribe(s);
        T v = s.blockingGet();
        return v != null ? v : defaultItem;
    }

    public final void blockingSubscribe() {
        BlockingConsumerIgnore s = new BlockingConsumerIgnore();
        subscribe(s);
        try {
            s.await();
        } catch (InterruptedException ex) {
            s.close();
            FolyamPlugins.onError(ex);
        }
    }

    public final void blockingSubscribe(CheckedConsumer<? super T> onNext) {
        blockingSubscribe(onNext, FolyamPlugins::onError, () -> { });
    }

    public final void blockingSubscribe(CheckedConsumer<? super T> onNext, CheckedConsumer<? super Throwable> onError) {
        blockingSubscribe(onNext, onError, () -> { });
    }

    public final void blockingSubscribe(CheckedConsumer<? super T> onNext, CheckedConsumer<? super Throwable> onError, CheckedRunnable onComplete) {
        Objects.requireNonNull(onNext, "onNext == null");
        Objects.requireNonNull(onError, "onError == null");
        Objects.requireNonNull(onComplete, "onComplete == null");

        BlockingLambdaConsumer<T> s = new BlockingLambdaConsumer<>(onNext, onError, onComplete, 1);
        subscribe(s);
        s.run();
    }

    public final Iterable<T> blockingIterable() {
        return new FolyamBlockingIterable<>(this, 1);
    }

    public final Stream<T> blockingStream() {
        return FolyamBlockingIterable.toStream(this, 1, false);
    }

    public final Future<T> toCompletableFuture() {
        CompletionStageConsumer<T> c = new CompletionStageConsumer<>();
        subscribe(c);
        return c;
    }
}
