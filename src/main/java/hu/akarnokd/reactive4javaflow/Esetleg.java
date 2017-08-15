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

import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.consumers.LambdaConsumer;
import hu.akarnokd.reactive4javaflow.impl.consumers.SafeFolyamSubscriber;
import hu.akarnokd.reactive4javaflow.impl.consumers.StrictSubscriber;
import hu.akarnokd.reactive4javaflow.impl.operators.*;
import hu.akarnokd.reactive4javaflow.impl.schedulers.ImmediateSchedulerService;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Stream;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
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

    public final <R> Esetleg<R> compose(Function<? super Esetleg<T>, ? extends Esetleg<R>> composer) {
        return to(composer);
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
        return FolyamPlugins.onAssembly(new EsetlegZipArray<>(new Esetleg[] { source1, source2 }, a -> zipper.apply((T)a[0], (U)a[1]), false));
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> Esetleg<R> zipDelayError(Esetleg<T> source1, Esetleg<U> source2, CheckedBiFunction<? super T, ? super U, ? extends R> zipper) {
        Objects.requireNonNull(source1, "sources1 == null");
        Objects.requireNonNull(source2, "sources2 == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new EsetlegZipArray<>(new Esetleg[] { source1, source2 }, a -> zipper.apply((T)a[0], (U)a[1]), true));
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

    // mappers of inner flows

    public final <R> Esetleg<R> flatMap(CheckedFunction<? super T, ? extends Esetleg<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> flatMapPublisher(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> flatMapIterable(CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> flatMapStream(CheckedFunction<? super T, ? extends Stream<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
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
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> delay(CheckedFunction<? super T, ? extends Flow.Publisher<?>> delaySelector) {
        Objects.requireNonNull(delaySelector, "delaySelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // state-peeking operators

    public final Esetleg<T> doOnSubscribe(CheckedConsumer<? super Flow.Subscription> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> doOnNext(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> doAfterNext(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> doOnError(CheckedConsumer<? super Throwable> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> doOnComplete(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
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
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> doOnCancel(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // resilience operators

    public final Esetleg<T> timeout(Flow.Publisher<?> firstTimeout) {
        Objects.requireNonNull(firstTimeout, "firstTimeout == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> timeout(Flow.Publisher<?> firstTimeout, Flow.Publisher<? extends T> fallback) {
        Objects.requireNonNull(firstTimeout, "firstTimeout == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> onErrorComplete() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> onErrorReturn(T item) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> onErrorFallback(Flow.Publisher<? extends T> fallback) {
        Objects.requireNonNull(fallback, "fallback == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> onErrorResumeNext(CheckedFunction<? super Throwable, ? extends Flow.Publisher<? extends T>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
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
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> retryWhen(Function<? super Folyam<Throwable>, ? extends Flow.Publisher<?>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
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

    public final Esetleg<T> minWith(Esetleg<? extends T> other, Comparator<? super T> comparator) {
        Objects.requireNonNull(other, "other == null");
        Objects.requireNonNull(comparator, "comparator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> maxWith(Esetleg<? extends T> other, Comparator<? super T> comparator) {
        Objects.requireNonNull(other, "other == null");
        Objects.requireNonNull(comparator, "comparator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // cold-hot conversion operators

    public final ConnectableFolyam<T> publish() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Esetleg<R> publish(CheckedFunction<? super Esetleg<T>, ? extends Flow.Publisher<? extends R>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> cache() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final ConnectableFolyam<T> replay() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Esetleg<R> replay(CheckedFunction<? super Esetleg<T>, ? extends Flow.Publisher<? extends R>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <U, R> Esetleg<R> multicast(CheckedFunction<? super Esetleg<T>, ? extends ConnectableFolyam<U>> multicaster, CheckedFunction<? super Folyam<U>, ? extends Flow.Publisher<? extends R>> handler) {
        Objects.requireNonNull(multicaster, "multicaster == null");
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // -----------------------------------------------------------------------------------
    // Blocking operators
    // -----------------------------------------------------------------------------------

    public final Optional<T> blockingGet() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final T blockingGet(T defaultItem) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final void blockingSubscribe() {
        blockingSubscribe(v -> { }, FolyamPlugins::onError, () -> { });
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

        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Iterable<T> blockingIterable() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Stream<T> blockingStream() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final CompletionStage<T> toCompletionStage() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Future<T> toFuture() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
