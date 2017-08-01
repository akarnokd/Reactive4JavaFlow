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
import hu.akarnokd.reactive4javaflow.impl.consumers.*;
import hu.akarnokd.reactive4javaflow.impl.operators.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public abstract class Folyam<T> implements Flow.Publisher<T> {

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

    public final void subscribe(FolyamSubscriber<? super T> s) {
        s = Objects.requireNonNull(FolyamPlugins.onSubscribe(this, s), "The plugin onSubscribe handler returned a null value");
        try {
            subscribeActual(s);
        } catch (Throwable ex) {
            FolyamPlugins.onError(ex);
        }
    }

    protected abstract void subscribeActual(FolyamSubscriber<? super T> s);

    public final <R> R to(Function<? super Folyam<T>, R> converter) {
        return converter.apply(this);
    }

    public final <R> Folyam<R> compose(Function<? super Folyam<T>, ? extends Folyam<R>> composer) {
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

    @SuppressWarnings("unchecked")
    public final void safeSubscribe(Flow.Subscriber<? super T> s) {
        Objects.requireNonNull(s, "s == null");
        if (s instanceof FolyamSubscriber) {
            subscribe(new SafeFolyamSubscriber<>((FolyamSubscriber<? super T>)s));
        } else {
            subscribe(new SafeFolyamSubscriber<>(new StrictSubscriber<>(s)));
        }
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

    public final <E extends Flow.Subscriber<? super T>> E subscribeWith(E s) {
        subscribe(s);
        return s;
    }

    // -----------------------------------------------------------------------------------
    // Source operators
    // -----------------------------------------------------------------------------------

    public static <T> Folyam<T> just(T item) {
        Objects.requireNonNull(item, "item == null");
        return FolyamPlugins.onAssembly(new FolyamJust<>(item));
    }

    @SuppressWarnings("unchecked")
    public static <T> Folyam<T> empty() {
        return FolyamPlugins.onAssembly((Folyam<T>) FolyamEmpty.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    public static <T> Folyam<T> never() {
        return FolyamPlugins.onAssembly((Folyam<T>) FolyamNever.INSTANCE);
    }

    public static <T> Folyam<T> error(Throwable error) {
        Objects.requireNonNull(error, "error == null");
        return FolyamPlugins.onAssembly(new FolyamError<>(error));
    }

    public static <T> Folyam<T> error(Callable<? extends Throwable> errorSupplier) {
        Objects.requireNonNull(errorSupplier, "errorSupplier == null");
        return FolyamPlugins.onAssembly(new FolyamErrorCallable<>(errorSupplier));
    }

    public static Folyam<Integer> range(int start, int count) {
        if (count == 0) {
            return empty();
        }
        if (count == 1) {
            return just(start);
        }
        if ((long)start + count - 1 > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException("start + count overflow");
        }
        return FolyamPlugins.onAssembly(new FolyamRange(start, start + count));
    }

    public static Folyam<Long> rangeLong(long start, long count) {
        if (count == 0L) {
            return empty();
        }
        if (count == 1L) {
            return just(start);
        }
        if (start > 0 && start + (count - 1) < 0L) {
            throw new IndexOutOfBoundsException("start + count overflow");
        }
        return FolyamPlugins.onAssembly(new FolyamRangeLong(start, start + count));
    }

    public static <T> Folyam<T> create(CheckedConsumer<? super FolyamEmitter<T>> onSubscribe, BackpressureHandling mode) {
        Objects.requireNonNull(onSubscribe, "onSubscribe == null");
        Objects.requireNonNull(mode, "mode == null");
        return FolyamPlugins.onAssembly(new FolyamCreate<>(onSubscribe, mode));
    }

    public static <T> Folyam<T> repeatItem(T item) {
        Objects.requireNonNull(item, "item == null");
        return FolyamPlugins.onAssembly(new FolyamRepeatItem<>(item));
    }

    public static <T> Folyam<T> repeatCallable(Callable<? extends T> callable) {
        Objects.requireNonNull(callable, "callable == null");
        return FolyamPlugins.onAssembly(new FolyamRepeatCallable<>(callable));
    }

    public static <T> Folyam<T> generate(CheckedConsumer<Emitter<T>> generator) {
        Objects.requireNonNull(generator, "generator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, S> Folyam<T> generate(Callable<S> stateSupplier, CheckedBiConsumer<S, Emitter<T>> generator) {
        Objects.requireNonNull(stateSupplier, "stateSupplier == null");
        Objects.requireNonNull(generator, "generator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, S> Folyam<T> generate(Callable<S> stateSupplier, CheckedBiConsumer<S, Emitter<T>> generator, CheckedConsumer<? super S> stateCleanup) {
        Objects.requireNonNull(stateSupplier, "stateSupplier == null");
        Objects.requireNonNull(generator, "generator == null");
        Objects.requireNonNull(stateCleanup, "stateCleanup == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, S> Folyam<T> generate(Callable<S> stateSupplier, CheckedBiFunction<S, Emitter<T>, S> generator) {
        Objects.requireNonNull(stateSupplier, "stateSupplier == null");
        Objects.requireNonNull(generator, "generator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, S> Folyam<T> generate(Callable<S> stateSupplier, CheckedBiFunction<S, Emitter<T>, S> generator, CheckedConsumer<? super S> stateCleanup) {
        Objects.requireNonNull(stateSupplier, "stateSupplier == null");
        Objects.requireNonNull(generator, "generator == null");
        Objects.requireNonNull(stateCleanup, "stateCleanup == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SafeVarargs
    public static <T> Folyam<T> fromArray(T... items) {
        Objects.requireNonNull(items, "items == null");
        int c = items.length;
        if (c == 0) {
            return empty();
        }
        return FolyamPlugins.onAssembly(new FolyamArray<>(items, 0, c));
    }

    @SafeVarargs
    public static <T> Folyam<T> fromArrayRange(int start, int end, T... items) {
        Objects.requireNonNull(items, "items == null");
        int c = items.length;
        if (start < 0 || end < 0 || start > end || start > c || end > c) {
            throw new IndexOutOfBoundsException("start: " + start + ", end: " + end + ", length: " + c);
        }
        return FolyamPlugins.onAssembly(new FolyamArray<>(items, start, end));
    }

    public static <T> Folyam<T> fromCallable(Callable<? extends T> callable) {
        Objects.requireNonNull(callable, "callable == null");
        return FolyamPlugins.onAssembly(new FolyamCallable<>(callable));
    }

    public static <T> Folyam<T> fromCompletionStage(CompletionStage<? extends T> stage) {
        Objects.requireNonNull(stage, "stage == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T> Folyam<T> fromFuture(Future<? extends T> future) {
        Objects.requireNonNull(future, "future == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T> Folyam<T> fromFuture(Future<? extends T> future, long timeout, TimeUnit unit) {
        Objects.requireNonNull(future, "future == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T> Folyam<T> fromIterable(Iterable<? extends T> iterable) {
        Objects.requireNonNull(iterable, "iterable == null");
        return FolyamPlugins.onAssembly(new FolyamIterable<>(iterable));
    }

    public static <T> Folyam<T> fromStream(Stream<? extends T> stream) {
        Objects.requireNonNull(stream, "stream == null");
        return FolyamPlugins.onAssembly(new FolyamIterable<T>(() -> (Iterator<T>)stream.iterator()));
    }

    public static <T> Folyam<T> fromOptional(Optional<? extends T> optional) {
        Objects.requireNonNull(optional, "optional == null");
        return optional.isPresent() ? just(optional.get()) : empty();
    }

    public static <T> Folyam<T> fromPublisher(Flow.Publisher<? extends T> source) {
        Objects.requireNonNull(source, "source == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static Folyam<Long> interval(long delay, TimeUnit unit, SchedulerService executor) {
        return interval(delay, delay, unit, executor);
    }

    public static Folyam<Long> interval(long initialDelay, long period, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static Folyam<Long> intervalRange(long start, long count, long initialDelay, long period, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static Folyam<Long> timer(long delay, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T> Folyam<T> defer(Callable<? extends Flow.Publisher<T>> publisherFactory) {
        Objects.requireNonNull(publisherFactory, "publisherFactory == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<T> using(Callable<R> resourceSupplier, CheckedFunction<? super R, ? extends Flow.Publisher<? extends T>> flowSupplier, CheckedConsumer<? super R> resourceCleaner) {
        return using(resourceSupplier, flowSupplier, resourceCleaner, false);
    }

    public static <T, R> Folyam<T> using(Callable<R> resourceSupplier, CheckedFunction<? super R, ? extends Flow.Publisher<? extends T>> flowSupplier, CheckedConsumer<? super R> resourceCleaner, boolean eagerCleanup) {
        Objects.requireNonNull(resourceSupplier, "resourceSupplier == null");
        Objects.requireNonNull(flowSupplier, "flowSupplier == null");
        Objects.requireNonNull(resourceCleaner, "resourceCleaner == null");

        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // -----------------------------------------------------------------------------------
    // Static combinator operators
    // -----------------------------------------------------------------------------------

    public static <T> Folyam<T> amb(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> combineLatest(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> combiner) {
        return combineLatest(sources, combiner, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> combineLatest(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> combiner, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(combiner, "combiner == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> combineLatestDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> combiner) {
        return combineLatestDelayError(sources, combiner, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> combineLatestDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> combiner, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(combiner, "combiner == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> concat(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return concat(sources, 2);
    }

    public static <T, R> Folyam<R> concat(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> concat(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        return concat(sources, 2);
    }

    public static <T, R> Folyam<R> concat(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> concatDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return concatDelayError(sources, 2);
    }

    public static <T, R> Folyam<R> concatDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> concatDelayError(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        return concatDelayError(sources, 2);
    }

    public static <T, R> Folyam<R> concatDelayError(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> merge(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return merge(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> merge(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> merge(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        return merge(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> merge(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> mergeDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return mergeDelayError(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> mergeDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> mergeDelayError(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        return mergeDelayError(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> mergeDelayError(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> zip(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper) {
        return zip(sources, zipper, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> zip(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> zipDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper) {
        return zipDelayError(sources, zipper, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> zipDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> zipLatest(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T extends Comparable<? super T>> Folyam<T> orderedMerge(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return orderedMerge(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T extends Comparable<? super T>> Folyam<T> orderedMerge(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T> Folyam<T> orderedMerge(Iterable<? extends Flow.Publisher<? extends T>> sources, Comparator<? super T> comparator) {
        return orderedMerge(sources, comparator, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> orderedMerge(Iterable<? extends Flow.Publisher<? extends T>> sources, Comparator<? super T> comparator, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(comparator, "comparator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T> Folyam<T> switchNext(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> concatEager(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return concatEager(sources, FolyamPlugins.defaultBufferSize(), FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> concatEager(Iterable<? extends Flow.Publisher<? extends T>> sources, int maxConcurrency) {
        return concatEager(sources, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> concatEager(Iterable<? extends Flow.Publisher<? extends T>> sources, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static <T, R> Folyam<R> concatEagerDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return concatEagerDelayError(sources, FolyamPlugins.defaultBufferSize(), FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> concatEagerDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, int maxConcurrency) {
        return concatEagerDelayError(sources, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> concatEagerDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SafeVarargs
    public static <T> Folyam<T> ambArray(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SafeVarargs
    public static <T> Folyam<T> concatArray(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SafeVarargs
    public static <T> Folyam<T> concatArrayDelayError(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SafeVarargs
    public static <T> Folyam<T> concatArrayEager(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SafeVarargs
    public static <T> Folyam<T> concatArrayEagerDelayError(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SafeVarargs
    public static <T> Folyam<T> mergeArray(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SafeVarargs
    public static <T> Folyam<T> mergeArrayDelayError(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SafeVarargs
    public static <T, R> Folyam<R> zipArray(CheckedFunction<? super Object[], ? extends R> zipper, Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SafeVarargs
    public static <T, R> Folyam<R> zipArrayDelayError(CheckedFunction<? super Object[], ? extends R> zipper, Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }


    // -----------------------------------------------------------------------------------
    // Instance operators
    // -----------------------------------------------------------------------------------

    public final <R> Folyam<R> map(CheckedFunction<? super T, ? extends R> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamMap<>(this, mapper));
    }

    public final <R> Folyam<R> mapOptional(CheckedFunction<? super T, ? extends Optional<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamMapOptional<>(this, mapper));
    }

    public final <R> Folyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return mapWhen(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <U, R> Folyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
        return mapWhen(mapper, combiner, FolyamPlugins.defaultBufferSize());
    }

    public final <U, R> Folyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        Objects.requireNonNull(combiner, "combiner == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> filter(CheckedPredicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        return FolyamPlugins.onAssembly(new FolyamFilter<>(this, predicate));
    }

    public final Folyam<T> filterWhen(CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter) {
        return filterWhen(filter, FolyamPlugins.defaultBufferSize());
    }

    public final Folyam<T> filterWhen(CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter, int prefetch) {
        Objects.requireNonNull(filter, "filter == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> take(long n) {
        return FolyamPlugins.onAssembly(new FolyamTake<>(this, n));
    }

    public final Folyam<T> takeLast(long n) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> skip(long n) {
        if (n <= 0L) {
            return this;
        }
        return FolyamPlugins.onAssembly(new FolyamSkip<>(this, n));
    }

    public final Folyam<T> skipLast(long n) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> takeWhile(CheckedPredicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> skipWhile(CheckedPredicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> takeUntil(CheckedPredicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> takeUntil(Flow.Publisher<?> other) {
        Objects.requireNonNull(other, "other == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> skipUntil(Flow.Publisher<?> other) {
        Objects.requireNonNull(other, "other == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> delaySubscription(Flow.Publisher<?> other) {
        Objects.requireNonNull(other, "other == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
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
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> repeatWhen(Function<? super Folyam<Object>, ? extends Flow.Publisher<?>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> switchIfEmpty(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> switchIfEmptyMany(Iterable<? extends Flow.Publisher<? extends T>> others) {
        Objects.requireNonNull(others, "others == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> defaultIfEmpty(T item) {
        Objects.requireNonNull(item, "item == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <U, R> Folyam<R> withLatestFrom(Flow.Publisher<? extends U> other, CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
        Objects.requireNonNull(other, "other == null");
        Objects.requireNonNull(combiner, "combiner == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <U, R> Folyam<R> withLatestFromMany(Iterable<? extends Flow.Publisher<? extends U>> others, CheckedFunction<? super Object[], ? extends R> combiner) {
        Objects.requireNonNull(others, "others == null");
        Objects.requireNonNull(combiner, "combiner == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> scan(CheckedBiFunction<T, T, T> scanner) {
        Objects.requireNonNull(scanner, "scanner == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> scan(Callable<? extends R> initialSupplier, CheckedBiFunction<R, ? super T, R> scanner) {
        Objects.requireNonNull(scanner, "scanner == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onTerminateDetach() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> rebatchRequests(long n) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> hide() {
        return FolyamPlugins.onAssembly(new FolyamHide<>(this));
    }

    // mappers of inner flows

    public final <R> Folyam<R> concatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return concatMap(mapper, 2);
    }

    public final <R> Folyam<R> concatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamConcatMap<>(this, mapper, prefetch, false));
    }

    public final <R> Folyam<R> concatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return concatMapDelayError(mapper, 2);
    }

    public final <R> Folyam<R> concatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamConcatMap<>(this, mapper, prefetch, true));
    }

    public final <R> Folyam<R> flatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return flatMap(mapper, FolyamPlugins.defaultBufferSize(), FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> flatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency) {
        return flatMap(mapper, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> flatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> flatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return flatMapDelayError(mapper, FolyamPlugins.defaultBufferSize(), FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> flatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency) {
        return flatMapDelayError(mapper, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> flatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> switchMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return switchMap(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> switchMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> switchMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return switchMapDelayError(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> switchMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> flatMapIterable(CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper) {
        return flatMapIterable(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> flatMapIterable(CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> flatMapStream(CheckedFunction<? super T, ? extends Stream<? extends R>> mapper) {
        return flatMapStream(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> flatMapStream(CheckedFunction<? super T, ? extends Stream<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> concatMapEager(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return concatMapEager(mapper, FolyamPlugins.defaultBufferSize(), FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> concatMapEager(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency) {
        return concatMapEager(mapper, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> concatMapEager(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> concatMapEagerDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return concatMapEagerDelayError(mapper, FolyamPlugins.defaultBufferSize(), FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> concatMapEagerDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency) {
        return concatMapEagerDelayError(mapper, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> concatMapEagerDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> valve(Flow.Publisher<Boolean> openClose) {
        return valve(openClose, FolyamPlugins.defaultBufferSize());
    }

    public final Folyam<T> valve(Flow.Publisher<Boolean> openClose, int prefetch) {
        Objects.requireNonNull(openClose, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // async-introducing operators

    public final Folyam<T> subscribeOn(SchedulerService executor) {
        return subscribeOn(executor, !(this instanceof FolyamCreate));
    }

    public final Folyam<T> subscribeOn(SchedulerService executor, boolean requestOn) {
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> observeOn(SchedulerService executor) {
        return observeOn(executor, FolyamPlugins.defaultBufferSize());
    }

    public final Folyam<T> observeOn(SchedulerService executor, int prefetch) {
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> delay(long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> delay(CheckedFunction<? super T, ? extends Flow.Publisher<?>> delaySelector) {
        Objects.requireNonNull(delaySelector, "delaySelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> spanout(long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // state-peeking operators

    public final Folyam<T> doOnSubscribe(CheckedConsumer<? super Flow.Subscription> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> doOnNext(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> doAfterNext(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> doOnError(CheckedConsumer<? super Throwable> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> doOnComplete(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> doFinally(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> doFinally(CheckedRunnable handler, SchedulerService executor) {
        Objects.requireNonNull(handler, "handler == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> doOnRequest(CheckedConsumer<? super Long> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> doOnCancel(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // custom backpressure handling

    public final Folyam<T> onBackpressureDrop() {
        return onBackpressureDrop(v -> { });
    }

    public final Folyam<T> onBackpressureDrop(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onBackpressureLatest() {
        return onBackpressureLatest(v -> { });
    }

    public final Folyam<T> onBackpressureLatest(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onBackpressureBuffer() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onBackpressureDropOldest(int capacity) {
        return onBackpressureDropOldest(capacity, v -> { });
    }

    public final Folyam<T> onBackpressureDropOldest(int capacity, CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onBackpressureDropNewest(int capacity) {
        return onBackpressureDropNewest(capacity, v -> { });
    }

    public final Folyam<T> onBackpressureDropNewest(int capacity, CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onBackpressureError() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onBackpressureError(int capacity) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onBackpressureTimeout(int capacity, long timeout, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onBackpressureTimeout(int capacity, long timeout, TimeUnit unit, SchedulerService executor, CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // resilience operators

    public final Folyam<T> timeout(CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeout) {
        Objects.requireNonNull(itemTimeout, "itemTimeout == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> timeout(CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeout, Flow.Publisher<? extends T> fallback) {
        Objects.requireNonNull(itemTimeout, "itemTimeout == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> timeout(Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeout) {
        Objects.requireNonNull(firstTimeout, "firstTimeout == null");
        Objects.requireNonNull(itemTimeout, "itemTimeout == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> timeout(Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeout, Flow.Publisher<? extends T> fallback) {
        Objects.requireNonNull(firstTimeout, "firstTimeout == null");
        Objects.requireNonNull(itemTimeout, "itemTimeout == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onErrorComplete() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onErrorReturn(T item) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onErrorFallback(Flow.Publisher<? extends T> fallback) {
        Objects.requireNonNull(fallback, "fallback == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> onErrorResumeNext(CheckedFunction<? super Throwable, ? extends Flow.Publisher<? extends T>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> retry() {
        return retry(Long.MAX_VALUE, e -> true);
    }

    public final Folyam<T> retry(long times) {
        return retry(times, e -> true);
    }

    public final Folyam<T> retry(CheckedPredicate<? super Throwable> condition) {
        return retry(Long.MAX_VALUE, condition);
    }

    public final Folyam<T> retry(long times, CheckedPredicate<? super Throwable> condition) {
        Objects.requireNonNull(condition, "condition == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> retryWhen(Function<? super Folyam<Throwable>, ? extends Flow.Publisher<?>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // pair combinators

    public final Folyam<T> startWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return concatArray(other, this);
    }

    public final Folyam<T> ambWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return ambArray(this, other);
    }

    public final Folyam<T> concatWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return concatArray(this, other);
    }

    public final Folyam<T> mergeWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return mergeArray(this, other);
    }

    public final <U, R> Folyam<R> zipWith(Flow.Publisher<? extends T> other, CheckedBiFunction<? super T, ? super U, ? extends R> zipper) {
        Objects.requireNonNull(other, "other == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return zipArray(a -> zipper.apply((T)a[0], (U)a[1]), this, other);
    }

    // operators returning Esetleg

    public final Esetleg<T> ignoreElements() {
        return FolyamPlugins.onAssembly(new FolyamIgnoreElements<>(this));
    }

    public final Esetleg<T> first() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> single() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> last() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> elementAt(long index) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <C> Esetleg<C> collect(Callable<C> collectionSupplier, CheckedBiConsumer<C, ? super T> collector) {
        Objects.requireNonNull(collectionSupplier, "collectionSupplier == null");
        Objects.requireNonNull(collector, "collector == null");
        return FolyamPlugins.onAssembly(new FolyamCollect<>(this, collectionSupplier, collector));
    }

    public final <A, R> Esetleg<R> collect(Collector<T, A, R> collector) {
        Objects.requireNonNull(collector, "collector == null");
        return FolyamPlugins.onAssembly(new FolyamStreamCollector<>(this, collector));
    }

    public final Esetleg<List<T>> toList() {
        return collect(ArrayList::new, List::add);
    }

    public final Esetleg<T> reduce(CheckedBiFunction<T, T, T> reducer) {
        Objects.requireNonNull(reducer, "reducer == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Esetleg<R> reduce(Callable<? extends R> initialSupplier, CheckedBiFunction<R, ? super T, R> reducer) {
        Objects.requireNonNull(reducer, "reducer == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<Boolean> equalsWith(Flow.Publisher<? extends T> other) {
        return Esetleg.sequenceEqual(this, other);
    }

    public final Esetleg<Boolean> equalsWith(Flow.Publisher<? extends T> other, CheckedBiPredicate<? super T, ? super T> isEqual) {
        return Esetleg.sequenceEqual(this, other, isEqual);
    }

    // buffering operators

    public final Folyam<List<T>> buffer(int size) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<List<T>> buffer(int size, int skip) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <C extends Collection<? super T>> Folyam<C> buffer(int size, int skip, Callable<C> collectionSupplier) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<List<T>> buffer(Flow.Publisher<?> boundary) {
        Objects.requireNonNull(boundary, "boundary == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <C extends Collection<? super T>> Folyam<C> buffer(Flow.Publisher<?> boundary, Callable<C> collectionSupplier) {
        Objects.requireNonNull(boundary, "boundary == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<List<T>> buffer(Flow.Publisher<?> boundary, int maxSize) {
        Objects.requireNonNull(boundary, "boundary == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <C extends Collection<? super T>> Folyam<C> buffer(Flow.Publisher<?> boundary, Callable<C> collectionSupplier, int maxSize) {
        Objects.requireNonNull(boundary, "boundary == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <U> Folyam<List<T>> buffer(Flow.Publisher<U> start, CheckedFunction<? super U, ? extends Flow.Publisher<?>> end) {
        Objects.requireNonNull(start, "start == null");
        Objects.requireNonNull(end, "end == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <U, C extends Collection<? super T>> Folyam<C> buffer(Flow.Publisher<U> start, CheckedFunction<? super U, ? extends Flow.Publisher<?>> end, Callable<C> collectionSupplier) {
        Objects.requireNonNull(start, "start == null");
        Objects.requireNonNull(end, "end == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<List<T>> bufferWhile(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<List<T>> bufferUntil(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<List<T>> bufferSplit(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<Folyam<T>> window(int size) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<Folyam<T>> window(int size, int skip) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<Folyam<T>> window(Flow.Publisher<?> boundary) {
        Objects.requireNonNull(boundary, "boundary == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<Folyam<T>> window(Flow.Publisher<?> boundary, int maxSize) {
        Objects.requireNonNull(boundary, "boundary == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <U> Folyam<Folyam<T>> window(Flow.Publisher<U> start, CheckedFunction<? super U, ? extends Flow.Publisher<?>> end) {
        Objects.requireNonNull(start, "start == null");
        Objects.requireNonNull(end, "end == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <K> Folyam<GroupedFolyam<K, T>> groupBy(CheckedFunction<T, K> keySelector) {
        return groupBy(keySelector, v -> v, FolyamPlugins.defaultBufferSize());
    }

    public final <K, V> Folyam<GroupedFolyam<K, V>> groupBy(CheckedFunction<T, K> keySelector, CheckedFunction<? super T, ? extends V> valueSelector) {
        return groupBy(keySelector, valueSelector, FolyamPlugins.defaultBufferSize());
    }

    public final <K, V> Folyam<GroupedFolyam<K, V>> groupBy(CheckedFunction<T, K> keySelector, CheckedFunction<? super T, ? extends V> valueSelector, int prefetch) {
        Objects.requireNonNull(keySelector, "keySelector == null");
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // cold-hot conversion operators

    public final ConnectableFolyam<T> publish() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final ConnectableFolyam<T> publish(int prefetch) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> publish(CheckedFunction<? super Folyam<T>, ? extends Flow.Publisher<? extends R>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> cache() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final ConnectableFolyam<T> replay() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final ConnectableFolyam<T> replayLast(int count) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final ConnectableFolyam<T> replayLast(long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final ConnectableFolyam<T> replayLast(int count, long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> Folyam<R> replay(CheckedFunction<? super Folyam<T>, ? extends Flow.Publisher<? extends R>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <U, R> Folyam<R> multicast(CheckedFunction<? super Folyam<T>, ? extends ConnectableFolyam<U>> multicaster, CheckedFunction<? super Folyam<U>, ? extends Flow.Publisher<? extends R>> handler) {
        Objects.requireNonNull(multicaster, "multicaster == null");
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // emission reducing operators

    public final Folyam<T> sample(Flow.Publisher<?> sampler) {
        Objects.requireNonNull(sampler, "sampler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> debounce(CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemDebouncer) {
        Objects.requireNonNull(itemDebouncer, "itemDebouncer == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> throttleFirst(long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> throttleLast(long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> throttleWithTimeout(long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> distinct() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <K> Folyam<T> distinct(CheckedFunction<? super T, ? extends K> keySelector) {
        Objects.requireNonNull(keySelector, "keySelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <K> Folyam<T> distinct(CheckedFunction<? super T, ? extends K> keySelector, Callable<? extends Collection<? super K>> collectionProvider) {
        Objects.requireNonNull(keySelector, "keySelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> distinctUntilChanged() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <K> Folyam<T> distinctUntilChanged(CheckedFunction<? super T, ? extends K> keySelector) {
        Objects.requireNonNull(keySelector, "keySelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <K> Folyam<T> distinctUntilChanged(Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator, "comparator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // parallel

    public final ParallelFolyam<T> parallel() {
        return parallel(Runtime.getRuntime().availableProcessors(), FolyamPlugins.defaultBufferSize());
    }

    public final ParallelFolyam<T> parallel(int parallelism) {
        return parallel(parallelism, FolyamPlugins.defaultBufferSize());
    }

    public final ParallelFolyam<T> parallel(int parallelism, int prefetch) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // type-specific operators

    public static Folyam<Integer> characters(CharSequence source) {
        Objects.requireNonNull(source, "source == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> min(Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator, "comparator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> max(Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator, "comparator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<Integer> sumInt(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<Long> sumLong(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<Float> sumFloat(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<Double> sumDouble(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // -----------------------------------------------------------------------------------
    // Blocking operators
    // -----------------------------------------------------------------------------------

    public final Optional<T> blockingFirst() {
        BlockingFirstConsumer<T> c = new BlockingFirstConsumer<>();
        subscribe(c);
        return Optional.ofNullable(c.blockingGet());
    }

    public final Optional<T> blockingFirst(long timeout, TimeUnit unit) {
        BlockingFirstConsumer<T> c = new BlockingFirstConsumer<>();
        subscribe(c);
        return Optional.ofNullable(c.blockingGet(timeout, unit));
    }

    public final T blockingFirst(T defaultItem) {
        BlockingFirstConsumer<T> c = new BlockingFirstConsumer<>();
        subscribe(c);
        T v = c.blockingGet();
        return v != null ? v : defaultItem;
    }

    public final Optional<T> blockingLast() {
        BlockingLastConsumer<T> c = new BlockingLastConsumer<>();
        subscribe(c);
        return Optional.ofNullable(c.blockingGet());
    }

    public final Optional<T> blockingLast(long timeout, TimeUnit unit) {
        BlockingLastConsumer<T> c = new BlockingLastConsumer<>();
        subscribe(c);
        return Optional.ofNullable(c.blockingGet(timeout, unit));
    }

    public final T blockingLast(T defaultItem) {
        BlockingLastConsumer<T> c = new BlockingLastConsumer<>();
        subscribe(c);
        T v = c.blockingGet();
        return v != null ? v : defaultItem;
    }

    public final T blockingSingle() {
        BlockingSingleConsumer<T> c = new BlockingSingleConsumer<>();
        subscribe(c);
        T v = c.blockingGet();
        if (v == null) {
            throw new NoSuchElementException();
        }
        return v;
    }

    public final T blockingSingle(T defaultItem) {
        BlockingSingleConsumer<T> c = new BlockingSingleConsumer<>();
        subscribe(c);
        T v = c.blockingGet();
        return v != null ? v : defaultItem;
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
        return blockingIterable(FolyamPlugins.defaultBufferSize());
    }

    public final Iterable<T> blockingIterable(int prefetch) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Stream<T> blockingStream() {
        return blockingStream(FolyamPlugins.defaultBufferSize());
    }

    public final Stream<T> blockingStream(int prefetch) {
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
