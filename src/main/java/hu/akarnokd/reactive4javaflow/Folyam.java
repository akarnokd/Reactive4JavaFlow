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
import hu.akarnokd.reactive4javaflow.impl.schedulers.ImmediateSchedulerService;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public abstract class Folyam<T> implements FolyamPublisher<T> {

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
        return generate(() -> null, (s, e) -> { generator.accept(e); return null;  }, s -> { });
    }

    public static <T, S> Folyam<T> generate(Callable<S> stateSupplier, CheckedBiConsumer<S, Emitter<T>> generator) {
        Objects.requireNonNull(generator, "generator == null");
        return generate(stateSupplier, (s, e) -> { generator.accept(s, e); return s;  }, s -> { });
    }

    public static <T, S> Folyam<T> generate(Callable<S> stateSupplier, CheckedBiConsumer<S, Emitter<T>> generator, CheckedConsumer<? super S> stateCleanup) {
        Objects.requireNonNull(generator, "generator == null");
        return generate(stateSupplier, (s, e) -> { generator.accept(s, e); return s;  }, stateCleanup);
    }

    public static <T, S> Folyam<T> generate(Callable<S> stateSupplier, CheckedBiFunction<S, Emitter<T>, S> generator) {
        return generate(stateSupplier, generator, s -> { });
    }

    public static <T, S> Folyam<T> generate(Callable<S> stateSupplier, CheckedBiFunction<S, Emitter<T>, S> generator, CheckedConsumer<? super S> stateCleanup) {
        Objects.requireNonNull(stateSupplier, "stateSupplier == null");
        Objects.requireNonNull(generator, "generator == null");
        Objects.requireNonNull(stateCleanup, "stateCleanup == null");
        return FolyamPlugins.onAssembly(new FolyamGenerate<>(stateSupplier, generator, stateCleanup));
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

    public static <T> Folyam<T> fromCallableAllowEmpty(Callable<? extends T> callable) {
        Objects.requireNonNull(callable, "callable == null");
        return FolyamPlugins.onAssembly(new FolyamCallableAllowEmpty<>(callable));
    }

    public static <T> Folyam<T> fromCompletionStage(CompletionStage<? extends T> stage) {
        Objects.requireNonNull(stage, "stage == null");
        return FolyamPlugins.onAssembly(new FolyamCompletionStage<>(stage));
    }

    public static <T> Folyam<T> fromFuture(Future<? extends T> future) {
        Objects.requireNonNull(future, "future == null");
        return FolyamPlugins.onAssembly(new FolyamFuture<>(future, 0, null));
    }

    public static <T> Folyam<T> fromFuture(Future<? extends T> future, long timeout, TimeUnit unit) {
        Objects.requireNonNull(future, "future == null");
        Objects.requireNonNull(unit, "unit == null");
        return FolyamPlugins.onAssembly(new FolyamFuture<>(future, timeout, unit));
    }

    public static <T> Folyam<T> fromIterable(Iterable<? extends T> iterable) {
        Objects.requireNonNull(iterable, "iterable == null");
        return FolyamPlugins.onAssembly(new FolyamIterable<>(iterable));
    }

    @SuppressWarnings("unchecked")
    public static <T> Folyam<T> fromStream(Stream<? extends T> stream) {
        Objects.requireNonNull(stream, "stream == null");
        return FolyamPlugins.onAssembly(new FolyamIterable<>(() -> (Iterator<T>)stream.iterator()));
    }


    @SuppressWarnings("unchecked")
    public static <T> Folyam<T> fromStream(Stream<? extends T> stream, boolean close) {
        Objects.requireNonNull(stream, "stream == null");
        if (close) {
            return FolyamPlugins.onAssembly(new FolyamStream<>(stream));
        }
        return FolyamPlugins.onAssembly(new FolyamIterable<>(() -> (Iterator<T>)stream.iterator()));
    }

    public static <T> Folyam<T> fromOptional(Optional<? extends T> optional) {
        Objects.requireNonNull(optional, "optional == null");
        return optional.isPresent() ? just(optional.get()) : empty();
    }

    @SuppressWarnings("unchecked")
    public static <T> Folyam<T> fromPublisher(Flow.Publisher<? extends T> source) {
        Objects.requireNonNull(source, "source == null");
        if (source instanceof Folyam) {
            return (Folyam<T>)source;
        }
        return FolyamPlugins.onAssembly(new FolyamWrap<>(source));
    }

    public static Folyam<Long> interval(long delay, TimeUnit unit, SchedulerService executor) {
        return interval(delay, delay, unit, executor);
    }

    public static Folyam<Long> interval(long initialDelay, long period, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamInterval(initialDelay, period, unit, executor));
    }

    public static Folyam<Long> intervalRange(long start, long count, long initialDelay, long period, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamInterval(start, start + count, initialDelay, period, unit, executor));
    }

    public static Folyam<Long> timer(long delay, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamTimer(delay, unit, executor));
    }

    public static <T> Folyam<T> defer(Callable<? extends Flow.Publisher<T>> publisherFactory) {
        Objects.requireNonNull(publisherFactory, "publisherFactory == null");
        return FolyamPlugins.onAssembly(new FolyamDefer<>(publisherFactory));
    }

    public static <T, R> Folyam<T> using(Callable<R> resourceSupplier, CheckedFunction<? super R, ? extends Flow.Publisher<? extends T>> flowSupplier, CheckedConsumer<? super R> resourceCleaner) {
        return using(resourceSupplier, flowSupplier, resourceCleaner, false);
    }

    public static <T, R> Folyam<T> using(Callable<R> resourceSupplier, CheckedFunction<? super R, ? extends Flow.Publisher<? extends T>> flowSupplier, CheckedConsumer<? super R> resourceCleaner, boolean eagerCleanup) {
        Objects.requireNonNull(resourceSupplier, "resourceSupplier == null");
        Objects.requireNonNull(flowSupplier, "flowSupplier == null");
        Objects.requireNonNull(resourceCleaner, "resourceCleaner == null");
        return FolyamPlugins.onAssembly(new FolyamUsing<>(resourceSupplier, flowSupplier, resourceCleaner, eagerCleanup));
    }

    // -----------------------------------------------------------------------------------
    // Static combinator operators
    // -----------------------------------------------------------------------------------

    public static <T> Folyam<T> amb(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamAmbIterable<>(sources));
    }

    public static <T, R> Folyam<R> combineLatest(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> combiner) {
        return combineLatest(sources, combiner, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> combineLatest(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> combiner, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(combiner, "combiner == null");
        return FolyamPlugins.onAssembly(new FolyamCombineLatestIterable<>(sources, combiner, prefetch, false));
    }

    public static <T, R> Folyam<R> combineLatestDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> combiner) {
        return combineLatestDelayError(sources, combiner, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> combineLatestDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> combiner, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(combiner, "combiner == null");
        return FolyamPlugins.onAssembly(new FolyamCombineLatestIterable<>(sources, combiner, prefetch, true));
    }

    @SafeVarargs
    public static <T, R> Folyam<R> combineLatestArray(CheckedFunction<? super Object[], ? extends R> combiner, Flow.Publisher<? extends T>... sources) {
        return combineLatestArray(combiner, FolyamPlugins.defaultBufferSize(), sources);
    }

    @SafeVarargs
    public static <T, R> Folyam<R> combineLatestArray(CheckedFunction<? super Object[], ? extends R> combiner, int prefetch, Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(combiner, "combiner == null");
        return FolyamPlugins.onAssembly(new FolyamCombineLatest<>(sources, combiner, prefetch, false));
    }

    @SafeVarargs
    public static <T, R> Folyam<R> combineLatestArrayDelayError(CheckedFunction<? super Object[], ? extends R> combiner, Flow.Publisher<? extends T>... sources) {
        return combineLatestArrayDelayError(combiner, FolyamPlugins.defaultBufferSize(), sources);
    }

    @SafeVarargs
    public static <T, R> Folyam<R> combineLatestArrayDelayError(CheckedFunction<? super Object[], ? extends R> combiner, int prefetch, Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(combiner, "combiner == null");
        return FolyamPlugins.onAssembly(new FolyamCombineLatest<>(sources, combiner, prefetch, true));
    }

    public static <T> Folyam<T> concat(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamConcatIterable<>(sources, false));
    }

    public static <T> Folyam<T> concat(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        return concat(sources, 2);
    }

    public static <T> Folyam<T> concat(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamConcatPublisher<>(sources, prefetch, false));
    }

    public static <T> Folyam<T> concatDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamConcatIterable<>(sources, true));
    }

    public static <T> Folyam<T> concatDelayError(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        return concatDelayError(sources, 2);
    }

    public static <T> Folyam<T> concatDelayError(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamConcatPublisher<>(sources, prefetch, true));
    }

    public static <T> Folyam<T> merge(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return merge(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> merge(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamMergeIterable<>(sources, prefetch, true));
    }

    public static <T> Folyam<T> merge(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        return merge(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> merge(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamMergePublisher<>(sources, prefetch, false));
    }

    public static <T> Folyam<T> mergeDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return mergeDelayError(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> mergeDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamMergeIterable<>(sources, prefetch, true));
    }

    public static <T> Folyam<T> mergeDelayError(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        return mergeDelayError(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> mergeDelayError(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamMergePublisher<>(sources, prefetch, true));
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> Folyam<R> zip(Flow.Publisher<? extends T> source1, Flow.Publisher<? extends U> source2, CheckedBiFunction<? super T, ? super U, ? extends R> zipper) {
        Objects.requireNonNull(source1, "source1 == null");
        Objects.requireNonNull(source2, "source2 == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return zipArray(a -> zipper.apply((T)a[0], (U)a[1]), source1, source2);
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> Folyam<R> zip(Flow.Publisher<? extends T> source1, Flow.Publisher<? extends U> source2, CheckedBiFunction<? super T, ? super U, ? extends R> zipper, int prefetch) {
        Objects.requireNonNull(source1, "source1 == null");
        Objects.requireNonNull(source2, "source2 == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return zipArray(a -> zipper.apply((T)a[0], (U)a[1]), prefetch, source1, source2);
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> Folyam<R> zipDelayError(Flow.Publisher<? extends T> source1, Flow.Publisher<? extends U> source2, CheckedBiFunction<? super T, ? super U, ? extends R> zipper) {
        Objects.requireNonNull(source1, "source1 == null");
        Objects.requireNonNull(source2, "source2 == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return zipArrayDelayError(a -> zipper.apply((T)a[0], (U)a[1]), source1, source2);
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> Folyam<R> zipDelayError(Flow.Publisher<? extends T> source1, Flow.Publisher<? extends U> source2, CheckedBiFunction<? super T, ? super U, ? extends R> zipper, int prefetch) {
        Objects.requireNonNull(source1, "source1 == null");
        Objects.requireNonNull(source2, "source2 == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return zipArrayDelayError(a -> zipper.apply((T)a[0], (U)a[1]), prefetch, source1, source2);
    }

    public static <T, R> Folyam<R> zip(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper) {
        return zip(sources, zipper, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> zip(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new FolyamZipIterable<>(sources, zipper, prefetch, false));
    }

    public static <T, R> Folyam<R> zipDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper) {
        return zipDelayError(sources, zipper, FolyamPlugins.defaultBufferSize());
    }

    public static <T, R> Folyam<R> zipDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new FolyamZipIterable<>(sources, zipper, prefetch, true));
    }

    public static <T, R> Folyam<R> zipLatest(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new FolyamZipLatestIterable<>(sources, zipper));
    }

    @SafeVarargs
    public static <T, R> Folyam<R> zipLatestArray(CheckedFunction<? super Object[], ? extends R> zipper, Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return FolyamPlugins.onAssembly(new FolyamZipLatestArray<>(sources, zipper));
    }

    public static <T extends Comparable<? super T>> Folyam<T> orderedMerge(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return orderedMerge(sources, Comparator.naturalOrder(), FolyamPlugins.defaultBufferSize());
    }

    public static <T extends Comparable<? super T>> Folyam<T> orderedMerge(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        return orderedMerge(sources, Comparator.naturalOrder(), prefetch);
    }

    public static <T> Folyam<T> orderedMerge(Iterable<? extends Flow.Publisher<? extends T>> sources, Comparator<? super T> comparator) {
        return orderedMerge(sources, comparator, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> orderedMerge(Iterable<? extends Flow.Publisher<? extends T>> sources, Comparator<? super T> comparator, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(comparator, "comparator == null");
        return FolyamPlugins.onAssembly(new FolyamOrderedMergeIterable<>(sources, comparator, prefetch, false));
    }

    public static <T extends Comparable<? super T>> Folyam<T> orderedMergeDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return orderedMergeDelayError(sources, Comparator.naturalOrder(), FolyamPlugins.defaultBufferSize());
    }

    public static <T extends Comparable<? super T>> Folyam<T> orderedMergeDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        return orderedMergeDelayError(sources, Comparator.naturalOrder(), prefetch);
    }

    public static <T> Folyam<T> orderedMergeDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, Comparator<? super T> comparator) {
        return orderedMergeDelayError(sources, comparator, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> orderedMergeDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, Comparator<? super T> comparator, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(comparator, "comparator == null");
        return FolyamPlugins.onAssembly(new FolyamOrderedMergeIterable<>(sources, comparator, prefetch, true));
    }

    @SafeVarargs
    public static <T extends Comparable<? super T>> Folyam<T> orderedMergeArray(Flow.Publisher<? extends T>... sources) {
        return orderedMergeArray(Comparator.naturalOrder(), FolyamPlugins.defaultBufferSize(), sources);
    }

    @SafeVarargs
    public static <T extends Comparable<? super T>> Folyam<T> orderedMergeArray(int prefetch, Flow.Publisher<? extends T>... sources) {
        return orderedMergeArray(Comparator.naturalOrder(), prefetch, sources);
    }

    @SafeVarargs
    public static <T> Folyam<T> orderedMergeArray(Comparator<? super T> comparator, Flow.Publisher<? extends T>... sources) {
        return orderedMergeArray(comparator, FolyamPlugins.defaultBufferSize(), sources);
    }

    @SafeVarargs
    public static <T> Folyam<T> orderedMergeArray(Comparator<? super T> comparator, int prefetch, Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(comparator, "comparator == null");
        return FolyamPlugins.onAssembly(new FolyamOrderedMergeArray<>(sources, comparator, prefetch, false));
    }

    @SafeVarargs
    public static <T extends Comparable<? super T>> Folyam<T> orderedMergeArrayDelayError(Flow.Publisher<? extends T>... sources) {
        return orderedMergeArrayDelayError(Comparator.naturalOrder(), FolyamPlugins.defaultBufferSize(), sources);
    }

    @SafeVarargs
    public static <T extends Comparable<? super T>> Folyam<T> orderedMergeArrayDelayError(int prefetch, Flow.Publisher<? extends T>... sources) {
        return orderedMergeArrayDelayError(Comparator.naturalOrder(), prefetch, sources);
    }

    @SafeVarargs
    public static <T> Folyam<T> orderedMergeArrayDelayError(Comparator<? super T> comparator, Flow.Publisher<? extends T>... sources) {
        return orderedMergeArrayDelayError(comparator, FolyamPlugins.defaultBufferSize(), sources);
    }

    @SafeVarargs
    public static <T> Folyam<T> orderedMergeArrayDelayError(Comparator<? super T> comparator, int prefetch, Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        Objects.requireNonNull(comparator, "comparator == null");
        return FolyamPlugins.onAssembly(new FolyamOrderedMergeArray<>(sources, comparator, prefetch, true));
    }

    public static <T> Folyam<T> switchNext(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        return switchNext(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> switchNext(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamSwitchNext<>(sources, prefetch, false));
    }

    public static <T> Folyam<T> switchNextDelayError(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources) {
        return switchNextDelayError(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> switchNextDelayError(Flow.Publisher<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamSwitchNext<>(sources, prefetch, true));
    }

    public static <T> Folyam<T> concatEager(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return concatEager(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> concatEager(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamConcatIterableEager<>(sources, prefetch, false));
    }

    public static <T> Folyam<T> concatEagerDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources) {
        return concatEagerDelayError(sources, FolyamPlugins.defaultBufferSize());
    }

    public static <T> Folyam<T> concatEagerDelayError(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamConcatIterableEager<>(sources, prefetch, true));
    }

    @SafeVarargs
    public static <T> Folyam<T> ambArray(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamAmbArray<>(sources));
    }

    @SafeVarargs
    public static <T> Folyam<T> concatArray(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamConcatArray<>(sources, false));
    }

    @SafeVarargs
    public static <T> Folyam<T> concatArrayDelayError(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamConcatArray<>(sources, true));
    }

    @SafeVarargs
    public static <T> Folyam<T> concatArrayEager(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamConcatArrayEager<>(sources, false));
    }

    @SafeVarargs
    public static <T> Folyam<T> concatArrayEagerDelayError(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamConcatArrayEager<>(sources, true));
    }

    @SafeVarargs
    public static <T> Folyam<T> mergeArray(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamMergeArray<>(sources, false));
    }

    @SafeVarargs
    public static <T> Folyam<T> mergeArrayDelayError(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamMergeArray<>(sources, true));
    }

    @SafeVarargs
    public static <T, R> Folyam<R> zipArray(CheckedFunction<? super Object[], ? extends R> zipper, Flow.Publisher<? extends T>... sources) {
        return zipArray(zipper, FolyamPlugins.defaultBufferSize(), sources);
    }

    @SafeVarargs
    public static <T, R> Folyam<R> zipArray(CheckedFunction<? super Object[], ? extends R> zipper, int prefetch, Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamZipArray<>(sources, zipper, prefetch, false));
    }

    @SafeVarargs
    public static <T, R> Folyam<R> zipArrayDelayError(CheckedFunction<? super Object[], ? extends R> zipper, Flow.Publisher<? extends T>... sources) {
        return zipArrayDelayError(zipper, FolyamPlugins.defaultBufferSize(), sources);
    }

    @SafeVarargs
    public static <T, R> Folyam<R> zipArrayDelayError(CheckedFunction<? super Object[], ? extends R> zipper, int prefetch, Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        return FolyamPlugins.onAssembly(new FolyamZipArray<>(sources, zipper, prefetch, true));
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
        return mapWhen(mapper, (a, b) -> b, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return mapWhen(mapper, (a, b) -> b, prefetch);
    }

    public final <U, R> Folyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
        return mapWhen(mapper, combiner, FolyamPlugins.defaultBufferSize());
    }

    public final <U, R> Folyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        Objects.requireNonNull(combiner, "combiner == null");
        return FolyamPlugins.onAssembly(new FolyamMapWhen<>(this, mapper, combiner, prefetch, false));
    }

    public final <R> Folyam<R> mapWhenDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return mapWhenDelayError(mapper, (a, b) -> b, FolyamPlugins.defaultBufferSize());
    }

    public final <U, R> Folyam<R> mapWhenDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
        return mapWhenDelayError(mapper, combiner, FolyamPlugins.defaultBufferSize());
    }

    public final <U, R> Folyam<R> mapWhenDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        Objects.requireNonNull(combiner, "combiner == null");
        return FolyamPlugins.onAssembly(new FolyamMapWhen<>(this, mapper, combiner, prefetch, true));
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
        return FolyamPlugins.onAssembly(new FolyamFilterWhen<>(this, filter, prefetch, false));
    }

    public final Folyam<T> filterWhenDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter) {
        return filterWhenDelayError(filter, FolyamPlugins.defaultBufferSize());
    }

    public final Folyam<T> filterWhenDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter, int prefetch) {
        Objects.requireNonNull(filter, "filter == null");
        return FolyamPlugins.onAssembly(new FolyamFilterWhen<>(this, filter, prefetch, true));
    }

    public final Folyam<T> take(long n) {
        return FolyamPlugins.onAssembly(new FolyamTake<>(this, n));
    }

    public final Folyam<T> takeLast(int n) {
        if (n <= 0) {
            return FolyamPlugins.onAssembly(new FolyamIgnoreElementsFolyam<>(this));
        }
        if (n == 1) {
            return FolyamPlugins.onAssembly(new FolyamTakeLastOneFolyam<>(this));
        }
        return FolyamPlugins.onAssembly(new FolyamTakeLast<>(this, n));
    }

    public final Folyam<T> skip(long n) {
        if (n <= 0L) {
            return this;
        }
        return FolyamPlugins.onAssembly(new FolyamSkip<>(this, n));
    }

    public final Folyam<T> skipLast(int n) {
        if (n <= 0) {
            return this;
        }
        return FolyamPlugins.onAssembly(new FolyamSkipLast<>(this, n));
    }

    public final Folyam<T> takeWhile(CheckedPredicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        return FolyamPlugins.onAssembly(new FolyamTakeWhile<>(this, predicate));
    }

    public final Folyam<T> skipWhile(CheckedPredicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        return FolyamPlugins.onAssembly(new FolyamSkipWhile<>(this, predicate));
    }

    public final Folyam<T> takeUntil(CheckedPredicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        return FolyamPlugins.onAssembly(new FolyamTakeUntilPredicate<>(this, predicate));
    }

    public final Folyam<T> takeUntil(Flow.Publisher<?> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(new FolyamTakeUntil<>(this, other));
    }

    public final Folyam<T> skipUntil(Flow.Publisher<?> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(new FolyamSkipUntil<>(this, other));
    }

    public final Folyam<T> delaySubscription(Flow.Publisher<?> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(new FolyamDelaySubscription<>(this, other));
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

    public final Folyam<T> switchIfEmpty(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(new FolyamSwitchIfEmpty<>(this, other));
    }

    public final Folyam<T> switchIfEmptyMany(Iterable<? extends Flow.Publisher<? extends T>> others) {
        Objects.requireNonNull(others, "others == null");
        return FolyamPlugins.onAssembly(new FolyamSwitchIfEmptyMany<>(this, others));
    }

    public final Folyam<T> defaultIfEmpty(T item) {
        return switchIfEmpty(Folyam.just(item));
    }

    public final <U, R> Folyam<R> withLatestFrom(Flow.Publisher<? extends U> other, CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
        Objects.requireNonNull(other, "other == null");
        Objects.requireNonNull(combiner, "combiner == null");
        return FolyamPlugins.onAssembly(new FolyamWithLatestFrom<>(this, other, combiner));
    }

    public final <U, R> Folyam<R> withLatestFromMany(Iterable<? extends Flow.Publisher<? extends U>> others, CheckedFunction<? super Object[], ? extends R> combiner) {
        Objects.requireNonNull(others, "others == null");
        Objects.requireNonNull(combiner, "combiner == null");
        return FolyamPlugins.onAssembly(new FolyamWithLatestFromMany<>(this, others, combiner));
    }

    public final Folyam<T> scan(CheckedBiFunction<T, T, T> scanner) {
        Objects.requireNonNull(scanner, "scanner == null");
        return FolyamPlugins.onAssembly(new FolyamScan<>(this, scanner));
    }

    public final <R> Folyam<R> scan(Callable<? extends R> initialSupplier, CheckedBiFunction<R, ? super T, R> scanner) {
        return scan(initialSupplier, scanner, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> scan(Callable<? extends R> initialSupplier, CheckedBiFunction<R, ? super T, R> scanner, int prefetch) {
        Objects.requireNonNull(initialSupplier, "initialSupplier == null");
        Objects.requireNonNull(scanner, "scanner == null");
        return FolyamPlugins.onAssembly(new FolyamScanSeed<>(this, initialSupplier, scanner, prefetch));
    }

    public final Folyam<T> onTerminateDetach() {
        return FolyamPlugins.onAssembly(new FolyamOnTerminateDetach<>(this));
    }

    public final Folyam<T> rebatchRequests(int n) {
        return observeOn(ImmediateSchedulerService.INSTANCE, n);
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
        return FolyamPlugins.onAssembly(new FolyamFlatMap<>(this, mapper, maxConcurrency, prefetch, false));
    }

    public final <R> Folyam<R> flatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return flatMapDelayError(mapper, FolyamPlugins.defaultBufferSize(), FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> flatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency) {
        return flatMapDelayError(mapper, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> flatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamFlatMap<>(this, mapper, maxConcurrency, prefetch, true));
    }

    public final <R> Folyam<R> switchMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return switchMap(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> switchMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamSwitchMap<>(this, mapper, prefetch, false));
    }

    public final <R> Folyam<R> switchMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return switchMapDelayError(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> switchMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamSwitchMap<>(this, mapper, prefetch, true));
    }

    public final <R> Folyam<R> switchFlatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency) {
        return switchFlatMap(mapper, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> switchFlatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamSwitchFlatMap<>(this, mapper, maxConcurrency, prefetch));
    }

    public final <R> Folyam<R> flatMapIterable(CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper) {
        return flatMapIterable(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> flatMapIterable(CheckedFunction<? super T, ? extends Iterable<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamFlattenIterable<>(this, mapper, prefetch));
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
        return FolyamPlugins.onAssembly(new FolyamConcatMapEager<>(this, mapper, maxConcurrency, prefetch, false));
    }

    public final <R> Folyam<R> concatMapEagerDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return concatMapEagerDelayError(mapper, FolyamPlugins.defaultBufferSize(), FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> concatMapEagerDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency) {
        return concatMapEagerDelayError(mapper, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> concatMapEagerDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamConcatMapEager<>(this, mapper, maxConcurrency, prefetch, true));
    }

    public final Folyam<T> valve(Flow.Publisher<Boolean> openClose) {
        return valve(openClose, FolyamPlugins.defaultBufferSize(), true);
    }

    public final Folyam<T> valve(Flow.Publisher<Boolean> openClose, boolean defaultOpen) {
        return valve(openClose, FolyamPlugins.defaultBufferSize(), defaultOpen);
    }

    public final Folyam<T> valve(Flow.Publisher<Boolean> openClose, int prefetch) {
        return valve(openClose, prefetch, true);
    }

    public final Folyam<T> valve(Flow.Publisher<Boolean> openClose, int prefetch, boolean defaultOpen) {
        Objects.requireNonNull(openClose, "mapper == null");
        return FolyamPlugins.onAssembly(new FolyamValve<>(this, openClose, prefetch, defaultOpen));
    }

    /**
     * Emits elements from the source and then expands them into another layer of Publishers, emitting
     * those items recursively until all Publishers become empty in a depth-first manner.
     * @param expander the function that converts an element into a Publisher to be expanded
     * @return the new Folyam instance
     */
    public final Folyam<T> expand(CheckedFunction<? super T, ? extends Flow.Publisher<? extends T>> expander) {
        return expand(expander, true, FolyamPlugins.defaultBufferSize());
    }

    /**
     * Emits elements from the source and then expands them into another layer of Publishers, emitting
     * those items recursively until all Publishers become empty with the specified strategy.
     * @param expander the function that converts an element into a Publisher to be expanded
     * @param depthFirst the expansion strategy; depth-first (true) will recursively expand the first item until there is no
     *                 more expansion possible, then the second items, and so on;
     *                 breath-first (false) will first expand the main source, then runs the expanded
     *                 Publishers in sequence, then the 3rd level, and so on.
     * @return the new Folyam instance
     */
    public final Folyam<T> expand(CheckedFunction<? super T, ? extends Flow.Publisher<? extends T>> expander, boolean depthFirst) {
        return expand(expander, depthFirst, FolyamPlugins.defaultBufferSize());
    }

    /**
     * Emits elements from the source and then expands them into another layer of Publishers, emitting
     * those items recursively until all Publishers become empty with the specified strategy.
     * @param expander the function that converts an element into a Publisher to be expanded
     * @param depthFirst the expansion strategy; depth-first (true) will recursively expand the first item until there is no
     *                 more expansion possible, then the second items, and so on;
     *                 breath-first (false) will first expand the main source, then runs the expanded
     *                 Publishers in sequence, then the 3rd level, and so on.
     * @param capacityHint the capacity hint for the breath-first expansion
     * @return the new Folyam instance
     */
    public final Folyam<T> expand(CheckedFunction<? super T, ? extends Flow.Publisher<? extends T>> expander, boolean depthFirst, int capacityHint) {
        Objects.requireNonNull(expander, "expander is null");
        //Objects.verifyPositive(capacityHint, "capacityHint");
        return FolyamPlugins.onAssembly(new FolyamExpand<>(this, expander, depthFirst, capacityHint));
    }

    // async-introducing operators

    public final Folyam<T> subscribeOn(SchedulerService executor) {
        return subscribeOn(executor, !(this instanceof FolyamCreate));
    }

    public final Folyam<T> subscribeOn(SchedulerService executor, boolean requestOn) {
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamSubscribeOn<>(this, executor, requestOn));
    }

    public final Folyam<T> observeOn(SchedulerService executor) {
        return observeOn(executor, FolyamPlugins.defaultBufferSize());
    }

    public final Folyam<T> observeOn(SchedulerService executor, int prefetch) {
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamObserveOn<>(this, executor, prefetch));
    }

    public final Folyam<T> delay(long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamDelayTime<>(this, time, unit, executor));
    }

    public final Folyam<T> delay(CheckedFunction<? super T, ? extends Flow.Publisher<?>> delaySelector) {
        Objects.requireNonNull(delaySelector, "delaySelector == null");
        return FolyamPlugins.onAssembly(new FolyamDelaySelector<>(this, delaySelector));
    }

    public final Folyam<T> spanout(long time, TimeUnit unit, SchedulerService executor) {
        return spanout(0L, time, unit, executor);
    }

    public final Folyam<T> spanout(long initialSpan, long betweenSpan, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamSpanout<>(this, initialSpan, betweenSpan, unit, executor, false, FolyamPlugins.defaultBufferSize()));
    }

    public final Folyam<T> spanoutDelayError(long time, TimeUnit unit, SchedulerService executor) {
        return spanoutDelayError(0L, time, unit, executor);
    }


    public final Folyam<T> spanoutDelayError(long initialSpan, long betweenSpan, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamSpanout<>(this, initialSpan, betweenSpan, unit, executor, true, FolyamPlugins.defaultBufferSize()));
    }

    // state-peeking operators

    public final Folyam<T> doOnSubscribe(CheckedConsumer<? super Flow.Subscription> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(FolyamDoOnSignal.withOnSubscribe(this, handler));
    }

    public final Folyam<T> doOnNext(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(FolyamDoOnSignal.withOnNext(this, handler));
    }

    public final Folyam<T> doAfterNext(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(FolyamDoOnSignal.withOnAfterNext(this, handler));
    }

    public final Folyam<T> doOnError(CheckedConsumer<? super Throwable> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(FolyamDoOnSignal.withOnError(this, handler));
    }

    public final Folyam<T> doOnComplete(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(FolyamDoOnSignal.withOnComplete(this, handler));
    }

    public final Folyam<T> doFinally(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        return doFinally(handler, ImmediateSchedulerService.INSTANCE);
    }

    public final Folyam<T> doFinally(CheckedRunnable handler, SchedulerService executor) {
        Objects.requireNonNull(handler, "handler == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamDoFinally<>(this, handler, executor));
    }

    public final Folyam<T> doOnRequest(CheckedConsumer<? super Long> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(FolyamDoOnSignal.withOnRequest(this, handler));
    }

    public final Folyam<T> doOnCancel(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(FolyamDoOnSignal.withOnCancel(this, handler));
    }

    // custom backpressure handling

    public final Folyam<T> onBackpressureDrop() {
        return onBackpressureDrop(v -> { });
    }

    public final Folyam<T> onBackpressureDrop(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new FolyamOnBackpressureDrop<>(this, handler));
    }

    public final Folyam<T> onBackpressureLatest() {
        return onBackpressureLatest(v -> { });
    }

    public final Folyam<T> onBackpressureLatest(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new FolyamOnBackpressureLatest<>(this, handler));
    }

    public final Folyam<T> onBackpressureBuffer() {
        return onBackpressureBuffer(FolyamPlugins.defaultBufferSize());
    }

    public final Folyam<T> onBackpressureBuffer(int capacityHint) {
        return FolyamPlugins.onAssembly(new FolyamOnBackpressureBufferAll<>(this, capacityHint, false));
    }

    public final Folyam<T> onBackpressureDropOldest(int capacity) {
        return onBackpressureDropOldest(capacity, v -> { });
    }

    public final Folyam<T> onBackpressureDropOldest(int capacity, CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new FolyamOnBackpressureBufferDrop<>(this, capacity, false, handler));
    }

    public final Folyam<T> onBackpressureDropNewest(int capacity) {
        return onBackpressureDropNewest(capacity, v -> { });
    }

    public final Folyam<T> onBackpressureDropNewest(int capacity, CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new FolyamOnBackpressureBufferDrop<>(this, capacity, true, handler));
    }

    public final Folyam<T> onBackpressureError() {
        return FolyamPlugins.onAssembly(new FolyamOnBackpressureDrop<>(this, v -> { throw new IllegalStateException("The consumer is not ready to receive items"); }));
    }

    public final Folyam<T> onBackpressureError(int capacity) {
        return FolyamPlugins.onAssembly(new FolyamOnBackpressureBufferAll<>(this, capacity, true));
    }

    public final Folyam<T> onBackpressureTimeout(long timeout, TimeUnit unit, SchedulerService executor) {
        return onBackpressureTimeout(Integer.MAX_VALUE, timeout, unit, executor);
    }

    public final Folyam<T> onBackpressureTimeout(int capacity, long timeout, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamOnBackpressureTimeout<>(this, capacity, timeout, unit, executor, null));
    }


    public final Folyam<T> onBackpressureTimeout(long timeout, TimeUnit unit, SchedulerService executor, CheckedConsumer<? super T> onEvict) {
        return onBackpressureTimeout(Integer.MAX_VALUE, timeout, unit, executor, onEvict);
    }

    public final Folyam<T> onBackpressureTimeout(int capacity, long timeout, TimeUnit unit, SchedulerService executor, CheckedConsumer<? super T> onEvict) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        Objects.requireNonNull(onEvict, "onEvict == null");
        return FolyamPlugins.onAssembly(new FolyamOnBackpressureTimeout<>(this, capacity, timeout, unit, executor, onEvict));
    }

    // resilience operators

    public final Folyam<T> timeout(long timeout, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamTimeoutTimed<>(this, timeout, unit, executor));
    }

    public final Folyam<T> timeout(long timeout, TimeUnit unit, SchedulerService executor, Flow.Publisher<? extends T> fallback) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        Objects.requireNonNull(fallback, "fallback == null");
        return FolyamPlugins.onAssembly(new FolyamTimeoutTimedFallback<>(this, timeout, unit, executor, fallback));
    }

    public final Folyam<T> timeout(CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector) {
        Objects.requireNonNull(itemTimeoutSelector, "itemTimeoutSelector == null");
        return FolyamPlugins.onAssembly(new FolyamTimeoutSelector<>(this, null, itemTimeoutSelector));
    }

    public final Folyam<T> timeout(CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector, Flow.Publisher<? extends T> fallback) {
        Objects.requireNonNull(itemTimeoutSelector, "itemTimeoutSelector == null");
        Objects.requireNonNull(fallback, "fallback == null");
        return FolyamPlugins.onAssembly(new FolyamTimeoutSelectorFallback<>(this, null, itemTimeoutSelector, fallback));
    }

    public final Folyam<T> timeout(Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector) {
        Objects.requireNonNull(firstTimeout, "firstTimeout == null");
        Objects.requireNonNull(itemTimeoutSelector, "itemTimeoutSelector == null");
        return FolyamPlugins.onAssembly(new FolyamTimeoutSelector<>(this, firstTimeout, itemTimeoutSelector));
    }

    public final Folyam<T> timeout(Flow.Publisher<?> firstTimeout, CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemTimeoutSelector, Flow.Publisher<? extends T> fallback) {
        Objects.requireNonNull(firstTimeout, "firstTimeout == null");
        Objects.requireNonNull(itemTimeoutSelector, "itemTimeoutSelector == null");
        Objects.requireNonNull(fallback, "fallback == null");
        return FolyamPlugins.onAssembly(new FolyamTimeoutSelectorFallback<>(this, firstTimeout, itemTimeoutSelector, fallback));
    }

    public final Folyam<T> onErrorComplete() {
        return FolyamPlugins.onAssembly(new FolyamOnErrorComplete<>(this));
    }

    public final Folyam<T> onErrorReturn(T item) {
        Objects.requireNonNull(item, "item == null");
        return onErrorFallback(Folyam.just(item));
    }

    public final Folyam<T> onErrorFallback(Flow.Publisher<? extends T> fallback) {
        Objects.requireNonNull(fallback, "fallback == null");
        return onErrorResumeNext(e -> fallback);
    }

    public final Folyam<T> onErrorResumeNext(CheckedFunction<? super Throwable, ? extends Flow.Publisher<? extends T>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new FolyamOnErrorResumeNext<>(this, handler));
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
        return FolyamPlugins.onAssembly(new FolyamRetry<>(this, times, condition));
    }

    public final Folyam<T> retryWhen(CheckedFunction<? super Folyam<Throwable>, ? extends Flow.Publisher<?>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new FolyamRetryWhen<>(this, handler));
    }

    // pair combinators

    public final Folyam<T> ambWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(FolyamAmbArray.ambWith(this, other));
    }

    public final Folyam<T> startWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(FolyamConcatArray.startWith(this, other, false));
    }

    public final Folyam<T> startWithDelayError(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(FolyamConcatArray.startWith(this, other, true));
    }

    public final Folyam<T> concatWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(FolyamConcatArray.concatWith(this, other, false));
    }

    public final Folyam<T> concatWithDelayError(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(FolyamConcatArray.concatWith(this, other, true));
    }

    public final Folyam<T> mergeWith(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(FolyamMergeArray.mergeWith(this, other, false));
    }

    public final Folyam<T> mergeWithDelayError(Flow.Publisher<? extends T> other) {
        Objects.requireNonNull(other, "other == null");
        return FolyamPlugins.onAssembly(FolyamMergeArray.mergeWith(this, other, true));
    }

    @SuppressWarnings("unchecked")
    public final <U, R> Folyam<R> zipWith(Flow.Publisher<? extends U> other, CheckedBiFunction<? super T, ? super U, ? extends R> zipper) {
        Objects.requireNonNull(other, "other == null");
        Objects.requireNonNull(zipper, "zipper == null");
        return zipArray(a -> zipper.apply((T)a[0], (U)a[1]), this, other);
    }

    // operators returning Esetleg

    public final Esetleg<T> ignoreElements() {
        return FolyamPlugins.onAssembly(new FolyamIgnoreElements<>(this));
    }

    public final Esetleg<T> first() {
        return elementAt(0L);
    }

    public final Esetleg<T> single() {
        return FolyamPlugins.onAssembly(new FolyamSingle<>(this, false));
    }

    public final Esetleg<T> esetleg() {
        return FolyamPlugins.onAssembly(new FolyamSingle<>(this, true));
    }

    public final Esetleg<T> last() {
        return FolyamPlugins.onAssembly(new FolyamTakeLastOne<>(this));
    }

    public final Esetleg<T> elementAt(long index) {
        return FolyamPlugins.onAssembly(new FolyamElementAt<>(this, index));
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
        return FolyamPlugins.onAssembly(new FolyamReduce<>(this, reducer));
    }

    public final <R> Esetleg<R> reduce(Callable<? extends R> initialSupplier, CheckedBiFunction<R, ? super T, R> reducer) {
        Objects.requireNonNull(reducer, "reducer == null");
        return FolyamPlugins.onAssembly(new FolyamReduceSeed<>(this, initialSupplier, reducer));
    }

    public final Esetleg<Boolean> equalsWith(Flow.Publisher<? extends T> other) {
        return Esetleg.sequenceEqual(this, other);
    }

    public final Esetleg<Boolean> equalsWith(Flow.Publisher<? extends T> other, CheckedBiPredicate<? super T, ? super T> isEqual) {
        return Esetleg.sequenceEqual(this, other, isEqual);
    }

    // buffering operators

    public final Folyam<List<T>> buffer(int size) {
        return buffer(size, size, ArrayList::new);
    }

    public final Folyam<List<T>> buffer(int size, int skip) {
        return buffer(size, skip, ArrayList::new);
    }

    public final <C extends Collection<? super T>> Folyam<C> buffer(int size, int skip, Callable<C> collectionSupplier) {
        Objects.requireNonNull(collectionSupplier, "collectionSupplier == null");
        return FolyamPlugins.onAssembly(new FolyamBufferSize<>(this, size, skip, collectionSupplier));
    }

    public final Folyam<List<T>> buffer(Flow.Publisher<?> boundary) {
        return buffer(boundary, ArrayList::new, Integer.MAX_VALUE);
    }

    public final <C extends Collection<? super T>> Folyam<C> buffer(Flow.Publisher<?> boundary, Callable<C> collectionSupplier) {
        return buffer(boundary, collectionSupplier, Integer.MAX_VALUE);
    }

    public final Folyam<List<T>> buffer(Flow.Publisher<?> boundary, int maxSize) {
        return buffer(boundary, ArrayList::new, maxSize);
    }

    public final <C extends Collection<? super T>> Folyam<C> buffer(Flow.Publisher<?> boundary, Callable<C> collectionSupplier, int maxSize) {
        Objects.requireNonNull(boundary, "boundary == null");
        Objects.requireNonNull(collectionSupplier, "collectionSupplier == null");
        return FolyamPlugins.onAssembly(new FolyamBufferBoundary<>(this, boundary, collectionSupplier, maxSize));
    }

    public final <U> Folyam<List<T>> buffer(Flow.Publisher<U> start, CheckedFunction<? super U, ? extends Flow.Publisher<?>> end) {
        return buffer(start, end, ArrayList::new);
    }

    public final <U, C extends Collection<? super T>> Folyam<C> buffer(Flow.Publisher<U> start, CheckedFunction<? super U, ? extends Flow.Publisher<?>> end, Callable<C> collectionSupplier) {
        Objects.requireNonNull(start, "start == null");
        Objects.requireNonNull(end, "end == null");
        Objects.requireNonNull(collectionSupplier, "collectionSupplier == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<List<T>> bufferWhile(CheckedPredicate<? super T> predicate) {
        return bufferWhile(predicate, ArrayList::new);
    }

    public final <C extends Collection<? super T>> Folyam<C> bufferWhile(CheckedPredicate<? super T> predicate, Callable<C> bufferSupplier) {
        Objects.requireNonNull(predicate, "predicate == null");
        Objects.requireNonNull(bufferSupplier, "bufferSupplier == null");
        return FolyamPlugins.onAssembly(new FolyamBufferPredicate<>(this, predicate, FolyamBufferPredicate.BufferPredicateMode.BEFORE, bufferSupplier));
    }

    public final Folyam<List<T>> bufferUntil(CheckedPredicate<? super T> predicate) {
        return bufferUntil(predicate, ArrayList::new);
    }

    public final <C extends Collection<? super T>> Folyam<C> bufferUntil(CheckedPredicate<? super T> predicate, Callable<C> bufferSupplier) {
        Objects.requireNonNull(predicate, "predicate == null");
        Objects.requireNonNull(bufferSupplier, "bufferSupplier == null");
        return FolyamPlugins.onAssembly(new FolyamBufferPredicate<>(this, predicate, FolyamBufferPredicate.BufferPredicateMode.AFTER, bufferSupplier));
    }

    public final Folyam<List<T>> bufferSplit(CheckedPredicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        return bufferSplit(predicate, ArrayList::new);
    }

    public final <C extends Collection<? super T>> Folyam<C> bufferSplit(CheckedPredicate<? super T> predicate, Callable<C> bufferSupplier) {
        Objects.requireNonNull(predicate, "predicate == null");
        Objects.requireNonNull(bufferSupplier, "bufferSupplier == null");
        return FolyamPlugins.onAssembly(new FolyamBufferPredicate<>(this, predicate, FolyamBufferPredicate.BufferPredicateMode.SPLIT, bufferSupplier));
    }

    public final Folyam<Folyam<T>> window(int size) {
        return window(size, size);
    }

    public final Folyam<Folyam<T>> window(int size, int skip) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<Folyam<T>> window(Flow.Publisher<?> boundary) {
        return window(boundary, Integer.MAX_VALUE);
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

    public final <K> Folyam<GroupedFolyam<K, T>> groupBy(CheckedFunction<? super T, ? extends K> keySelector) {
        return groupBy(keySelector, v -> v, FolyamPlugins.defaultBufferSize());
    }

    public final <K, V> Folyam<GroupedFolyam<K, V>> groupBy(CheckedFunction<? super T, ? extends K> keySelector, CheckedFunction<? super T, ? extends V> valueSelector) {
        return groupBy(keySelector, valueSelector, FolyamPlugins.defaultBufferSize());
    }

    public final <K, V> Folyam<GroupedFolyam<K, V>> groupBy(CheckedFunction<? super T, ? extends K> keySelector, CheckedFunction<? super T, ? extends V> valueSelector, int prefetch) {
        Objects.requireNonNull(keySelector, "keySelector == null");
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        return FolyamPlugins.onAssembly(new FolyamGroupBy<>(this, keySelector, valueSelector, prefetch));
    }

    // cold-hot conversion operators

    public final ConnectableFolyam<T> publish() {
        return publish(FolyamPlugins.defaultBufferSize());
    }

    public final ConnectableFolyam<T> publish(int prefetch) {
        return FolyamPlugins.onAssembly(new ConnectableFolyamPublish<>(this, prefetch));
    }

    public final <R> Folyam<R> publish(CheckedFunction<? super Folyam<T>, ? extends Flow.Publisher<? extends R>> handler) {
        return publish(handler, FolyamPlugins.defaultBufferSize());
    }

    public final <R> Folyam<R> publish(CheckedFunction<? super Folyam<T>, ? extends Flow.Publisher<? extends R>> handler, int prefetch) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new FolyamPublish<>(this, handler, prefetch));
    }

    public final Folyam<T> cache() {
        return replay().autoConnect();
    }

    public final ConnectableFolyam<T> replay() {
        return FolyamPlugins.onAssembly(new ConnectableFolyamReplayUnbounded<>(this, 16));
    }

    public final ConnectableFolyam<T> replayLast(int count) {
        return FolyamPlugins.onAssembly(new ConnectableFolyamReplaySizeBound<>(this, count));
    }

    public final ConnectableFolyam<T> replayLast(long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new ConnectableFolyamReplaySizeAndTimeBound<>(this, Integer.MAX_VALUE, time, unit, executor));
    }

    public final ConnectableFolyam<T> replayLast(int count, long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new ConnectableFolyamReplaySizeAndTimeBound<>(this, count, time, unit, executor));
    }

    public final <R> Folyam<R> replay(CheckedFunction<? super Folyam<T>, ? extends Flow.Publisher<? extends R>> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new FolyamReplay<>(this, handler, 16));
    }

    public final <U, R> Folyam<R> multicast(CheckedFunction<? super Folyam<T>, ? extends ConnectableFolyam<U>> multicaster, CheckedFunction<? super Folyam<U>, ? extends Flow.Publisher<? extends R>> handler) {
        Objects.requireNonNull(multicaster, "multicaster == null");
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new FolyamMulticast<>(this, multicaster, handler));
    }

    // emission reducing operators

    public final Folyam<T> sample(Flow.Publisher<?> sampler) {
        return sample(sampler, true);
    }

    public final Folyam<T> sample(Flow.Publisher<?> sampler, boolean emitLast) {
        Objects.requireNonNull(sampler, "sampler == null");
        return FolyamPlugins.onAssembly(new FolyamSample<>(this, sampler, emitLast));
    }

    public final Folyam<T> debounce(CheckedFunction<? super T, ? extends Flow.Publisher<?>> itemDebouncer) {
        Objects.requireNonNull(itemDebouncer, "itemDebouncer == null");
        return FolyamPlugins.onAssembly(new FolyamDebounce<>(this, itemDebouncer));
    }

    public final Folyam<T> throttleFirst(long time, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new FolyamThrottleFirstTime<>(this, time, unit, executor));
    }

    public final Folyam<T> throttleLast(long time, TimeUnit unit, SchedulerService executor) {
        return sample(interval(time, unit, executor));
    }

    public final Folyam<T> throttleLast(long time, TimeUnit unit, SchedulerService executor, boolean emitLast) {
        return sample(interval(time, unit, executor), emitLast);
    }

    public final Folyam<T> throttleWithTimeout(long time, TimeUnit unit, SchedulerService executor) {
        Folyam<Long> timer = timer(time, unit, executor);
        return debounce(v -> timer);
    }

    public final Folyam<T> distinct() {
        return distinct(v -> v, HashSet::new);
    }

    public final <K> Folyam<T> distinct(CheckedFunction<? super T, ? extends K> keySelector) {
        return distinct(keySelector, HashSet::new);
    }

    public final <K> Folyam<T> distinct(CheckedFunction<? super T, ? extends K> keySelector, Callable<? extends Collection<? super K>> collectionProvider) {
        Objects.requireNonNull(keySelector, "keySelector == null");
        return FolyamPlugins.onAssembly(new FolyamDistinct<>(this, keySelector, collectionProvider));
    }

    public final Folyam<T> distinctUntilChanged() {
        return distinctUntilChanged(Objects::equals);
    }

    public final Folyam<T> distinctUntilChanged(CheckedBiPredicate<? super T, ? super T> comparator) {
        Objects.requireNonNull(comparator, "comparator == null");
        return FolyamPlugins.onAssembly(new FolyamDistinctUntilChanged<>(this, comparator));
    }

    public final <K> Folyam<T> distinctUntilChanged(CheckedFunction<? super T, ? extends K> keySelector) {
        Objects.requireNonNull(keySelector, "keySelector == null");
        return distinctUntilChanged(keySelector, Objects::equals);
    }

    public final <K> Folyam<T> distinctUntilChanged(CheckedFunction<? super T, ? extends K> keySelector, CheckedBiPredicate<? super K, ? super K> comparator) {
        Objects.requireNonNull(keySelector, "keySelector == null");
        Objects.requireNonNull(comparator, "comparator == null");
        return FolyamPlugins.onAssembly(new FolyamDistinctUntilChangedSelector<>(this, keySelector, comparator));
    }

    // parallel

    public final ParallelFolyam<T> parallel() {
        return parallel(Runtime.getRuntime().availableProcessors(), FolyamPlugins.defaultBufferSize());
    }

    public final ParallelFolyam<T> parallel(int parallelism) {
        return parallel(parallelism, FolyamPlugins.defaultBufferSize());
    }

    public final ParallelFolyam<T> parallel(int parallelism, int prefetch) {
        return FolyamPlugins.onAssembly(new ParallelFromPublisher<>(this, parallelism, prefetch));
    }

    // type-specific operators

    public static Folyam<Integer> characters(CharSequence source) {
        Objects.requireNonNull(source, "source == null");
        return FolyamPlugins.onAssembly(new FolyamCharacters(source, 0, source.length()));
    }

    public static Folyam<Integer> characters(CharSequence source, int start, int end) {
        Objects.requireNonNull(source, "source == null");
        int c = source.length();
        if (start < 0 || end < 0 || start > end || start > c || end > c) {
            throw new IndexOutOfBoundsException("start: " + start + ", end: " + end + ", length: " + c);
        }
        return FolyamPlugins.onAssembly(new FolyamCharacters(source, start, end));
    }

    public final Esetleg<T> min(Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator, "comparator == null");
        return reduce((a, b) -> comparator.compare(a, b) < 0 ? a : b);
    }

    public final Esetleg<T> max(Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator, "comparator == null");
        return reduce((a, b) -> comparator.compare(a, b) > 0 ? a : b);
    }

    public final Esetleg<Integer> sumInt(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        return FolyamPlugins.onAssembly(new FolyamSumInt<>(this, valueSelector));
    }

    public final Esetleg<Long> sumLong(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        return FolyamPlugins.onAssembly(new FolyamSumLong<>(this, valueSelector));
    }

    public final Esetleg<Float> sumFloat(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        return FolyamPlugins.onAssembly(new FolyamSumFloat<>(this, valueSelector));
    }

    public final Esetleg<Double> sumDouble(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        return FolyamPlugins.onAssembly(new FolyamSumDouble<>(this, valueSelector));
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

        BlockingLambdaConsumer<T> s = new BlockingLambdaConsumer<>(onNext, onError, onComplete, FolyamPlugins.defaultBufferSize());
        subscribe(s);
        s.run();
    }

    public final Iterable<T> blockingIterable() {
        return blockingIterable(FolyamPlugins.defaultBufferSize());
    }

    public final Iterable<T> blockingIterable(int prefetch) {
        return new FolyamBlockingIterable<>(this, prefetch);
    }

    public final Stream<T> blockingStream() {
        return blockingStream(FolyamPlugins.defaultBufferSize());
    }

    public final Stream<T> blockingStream(int prefetch) {
        return FolyamBlockingIterable.toStream(this, prefetch, false);
    }

    public final CompletableFuture<T> toCompletableFuture() {
        CompletionStageConsumer<T> c = new CompletionStageConsumer<>();
        subscribe(c);
        return c;
    }
}
