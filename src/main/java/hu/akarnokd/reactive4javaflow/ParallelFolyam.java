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
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;
import hu.akarnokd.reactive4javaflow.impl.operators.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

public abstract class ParallelFolyam<T> {

    public abstract int parallelism();

    public final void subscribe(FolyamSubscriber<? super T>[] subscribers) {
        if (validate(subscribers)) {
            applyPlugins(subscribers);
            subscribeActual(subscribers);
        }
    }

    protected abstract void subscribeActual(FolyamSubscriber<? super T>[] subscribers);

    final boolean validate(FolyamSubscriber<? super T>[] subscribers) {
        int p = parallelism();
        if (p != subscribers.length) {
            IllegalArgumentException ex = new IllegalArgumentException("Subscribers expected: " + p + ", actual: " + subscribers.length);
            for (FolyamSubscriber<? super T> s : subscribers) {
                EmptySubscription.error(s, ex);
            }
            return false;
        }
        return true;
    }

    final void applyPlugins(FolyamSubscriber<? super T>[] subscribers) {
        for (int i = 0; i < subscribers.length; i++) {
            subscribers[i] = Objects.requireNonNull(
                    FolyamPlugins.onSubscribe(this, subscribers[i]),
                    "The plugin onSubscribe handler returned a null value"
                    );
        }
    }

    // ------------------------------------------------------------------------
    // General source operators
    // ------------------------------------------------------------------------

    public static <T> ParallelFolyam<T> fromPublisher(Flow.Publisher<? extends T> source) {
        return fromPublisher(source, Runtime.getRuntime().availableProcessors(), FolyamPlugins.defaultBufferSize());
    }

    public static <T> ParallelFolyam<T> fromPublisher(Flow.Publisher<? extends T> source, int parallelism) {
        Objects.requireNonNull(source, "source == null");
        return FolyamPlugins.onAssembly(new ParallelFromPublisher<>(source, parallelism, FolyamPlugins.defaultBufferSize()));
    }

    public static <T> ParallelFolyam<T> fromPublisher(Flow.Publisher<? extends T> source, int parallelism, int prefetch) {
        Objects.requireNonNull(source, "source == null");
        return FolyamPlugins.onAssembly(new ParallelFromPublisher<>(source, parallelism, prefetch));
    }

    @SafeVarargs
    public static <T> ParallelFolyam<T> fromArray(Flow.Publisher<? extends T>... sources) {
        Objects.requireNonNull(sources, "sources == null");
        if (sources.length == 0) {
            throw new IllegalArgumentException("sources.length == 0");
        }
        return FolyamPlugins.onAssembly(new ParallelFromArray<>(sources));
    }

    public static <T> ParallelFolyam<T> defer(Callable<? extends ParallelFolyam<? extends T>> callable, int parallelism) {
        Objects.requireNonNull(callable, "callable == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // ------------------------------------------------------------------------
    // Instance operators
    // ------------------------------------------------------------------------

    /**
     * Specifies where each 'rail' will observe its incoming values with
     * no work-stealing and default prefetch amount.
     * <p>
     * This operator uses the default prefetch size returned by {@code FolyamPlugins.defaultBufferSize()}.
     * <p>
     * The operator will call {@code SchedulerService.worker()} as many
     * times as this ParallelFolyam's parallelism level is.
     * <p>
     * No assumptions are made about the SchedulerService's parallelism level,
     * if the SchedulerService's parallelism level is lower than the ParallelFolyam's,
     * some rails may end up on the same thread/worker.
     * <p>
     * This operator doesn't require the SchedulerService to be trampolining as it
     * does its own built-in trampolining logic.
     *
     * @param scheduler the scheduler to use
     * @return the new ParallelFlowable instance
     */
    public final ParallelFolyam<T> runOn(SchedulerService scheduler) {
        return runOn(scheduler, FolyamPlugins.defaultBufferSize());
    }

    /**
     * Specifies where each 'rail' will observe its incoming values with
     * possibly work-stealing and a given prefetch amount.
     * <p>
     * The operator will call {@code SchedulerService.worker()} as many
     * times as this ParallelFolyam's parallelism level is.
     * <p>
     * No assumptions are made about the SchedulerService's parallelism level,
     * if the SchedulerService's parallelism level is lower than the ParallelFolyam's,
     * some rails may end up on the same thread/worker.
     * <p>
     * This operator doesn't require the SchedulerService to be trampolining as it
     * does its own built-in trampolining logic.
     *
     * @param scheduler the scheduler to use
     * that rail's worker has run out of work.
     * @param prefetch the number of values to request on each 'rail' from the source
     * @return the new ParallelFlowable instance
     */
    public final ParallelFolyam<T> runOn(SchedulerService scheduler, int prefetch) {
        Objects.requireNonNull(scheduler, "scheduler");
        // FIXME parameter validation
        // Objects.verifyPositive(prefetch, "prefetch");
        return FolyamPlugins.onAssembly(new ParallelRunOn<T>(this, scheduler, prefetch));
    }


    public final <R> ParallelFolyam<R> map(CheckedFunction<? super T, ? extends R> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new ParallelMap<>(this, mapper));
    }

    public final <R> ParallelFolyam<R> map(CheckedFunction<? super T, ? extends R> mapper, ParallelFailureHandling failureHandling) {
        Objects.requireNonNull(mapper, "mapper == null");
        Objects.requireNonNull(failureHandling, "failureHandling == null");
        return FolyamPlugins.onAssembly(new ParallelMapTry<>(this, mapper, failureHandling));
    }

    public final <R> ParallelFolyam<R> map(CheckedFunction<? super T, ? extends R> mapper, CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> failureHandling) {
        Objects.requireNonNull(mapper, "mapper == null");
        Objects.requireNonNull(failureHandling, "failureHandling == null");
        return FolyamPlugins.onAssembly(new ParallelMapTry<>(this, mapper, failureHandling));
    }

    public final <R> ParallelFolyam<R> mapOptional(CheckedFunction<? super T, ? extends Optional<R>> mapper) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> ParallelFolyam<R> mapOptional(CheckedFunction<? super T, ? extends Optional<R>> mapper, ParallelFailureHandling failureHandling) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> ParallelFolyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return mapWhen(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> ParallelFolyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <U, R> ParallelFolyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner) {
        return mapWhen(mapper, combiner, FolyamPlugins.defaultBufferSize());
    }

    public final <U, R> ParallelFolyam<R> mapWhen(CheckedFunction<? super T, ? extends Flow.Publisher<? extends U>> mapper, CheckedBiFunction<? super T, ? super U, ? extends R> combiner, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final ParallelFolyam<T> filter(CheckedPredicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate == null");
        return FolyamPlugins.onAssembly(new ParallelFilter<>(this, predicate));
    }

    public final ParallelFolyam<T> filter(CheckedPredicate<? super T> predicate, ParallelFailureHandling failureHandling) {
        Objects.requireNonNull(predicate, "predicate == null");
        Objects.requireNonNull(failureHandling, "failureHandling == null");
        return FolyamPlugins.onAssembly(new ParallelFilterTry<>(this, predicate, failureHandling));
    }

    public final ParallelFolyam<T> filter(CheckedPredicate<? super T> predicate, CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> failureHandling) {
        Objects.requireNonNull(predicate, "predicate == null");
        Objects.requireNonNull(failureHandling, "failureHandling == null");
        return FolyamPlugins.onAssembly(new ParallelFilterTry<>(this, predicate, failureHandling));
    }

    public final ParallelFolyam<T> filterWhen(CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> predicate) {
        return filterWhen(predicate, FolyamPlugins.defaultBufferSize());
    }

    public final ParallelFolyam<T> filterWhen(CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> predicate, int prefetch) {
        Objects.requireNonNull(predicate, "predicate == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <R> R to(Function<? super ParallelFolyam<T>, ? extends R> converter) {
        return converter.apply(this);
    }

    public final <R> ParallelFolyam<R> compose(Function<? super ParallelFolyam<T>, ? extends ParallelFolyam<R>> composer) {
        return to(composer);
    }

    // state peeking operators

    public final ParallelFolyam<T> doOnNext(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new ParallelPeek<>(this, handler, v -> { }, e -> { }, () -> { }, s -> { }, r -> { }, () -> { }));
    }

    public final ParallelFolyam<T> doOnNext(CheckedConsumer<? super T> handler, ParallelFailureHandling failureHandling) {
        Objects.requireNonNull(handler, "handler == null");
        Objects.requireNonNull(failureHandling, "failureHandling == null");
        return FolyamPlugins.onAssembly(new ParallelDoOnNextTry<>(this, handler, failureHandling));
    }

    public final ParallelFolyam<T> doOnNext(CheckedConsumer<? super T> handler, CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> failureHandling) {
        Objects.requireNonNull(handler, "handler == null");
        Objects.requireNonNull(failureHandling, "failureHandling == null");
        return FolyamPlugins.onAssembly(new ParallelDoOnNextTry<>(this, handler, failureHandling));
    }

    public final ParallelFolyam<T> doAfterNext(CheckedConsumer<? super T> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new ParallelPeek<>(this, v -> { }, handler, e -> { }, () -> { }, s -> { }, r -> { }, () -> { }));
    }

    public final ParallelFolyam<T> doOnError(CheckedConsumer<? super Throwable> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new ParallelPeek<>(this, v -> { }, v -> { }, handler, () -> { }, s -> { }, r -> { }, () -> { }));
    }

    public final ParallelFolyam<T> doOnComplete(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new ParallelPeek<>(this, v -> { }, v -> { }, e -> { }, handler, s -> { }, r -> { }, () -> { }));
    }

    public final ParallelFolyam<T> doOnCancel(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new ParallelPeek<>(this, v -> { }, v -> { }, e -> { }, () -> { }, s -> { }, r -> { }, handler));
    }

    public final ParallelFolyam<T> doOnSubscribe(CheckedConsumer<? super Flow.Subscription> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new ParallelPeek<>(this, v -> { }, v -> { }, e -> { }, () -> { }, handler, r -> { }, () -> { }));
    }

    public final ParallelFolyam<T> doOnRequest(CheckedConsumer<? super Long> handler) {
        Objects.requireNonNull(handler, "handler == null");
        return FolyamPlugins.onAssembly(new ParallelPeek<>(this, v -> { }, v -> { }, e -> { }, () -> { }, s -> { }, handler, () -> { }));
    }

    public final ParallelFolyam<T> doFinally(CheckedRunnable handler) {
        Objects.requireNonNull(handler, "handler == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    // // mappers of inner flows

    public final <R> ParallelFolyam<R> concatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return concatMap(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> ParallelFolyam<R> concatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new ParallelConcatMap<>(this, mapper, prefetch, false));
    }

    public final <R> ParallelFolyam<R> concatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return concatMapDelayError(mapper, FolyamPlugins.defaultBufferSize());
    }

    public final <R> ParallelFolyam<R> concatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new ParallelConcatMap<>(this, mapper, prefetch, true));
    }

    public final <R> ParallelFolyam<R> flatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return flatMap(mapper, FolyamPlugins.defaultBufferSize(), FolyamPlugins.defaultBufferSize());
    }

    public final <R> ParallelFolyam<R> flatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency) {
        return flatMap(mapper, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public final <R> ParallelFolyam<R> flatMap(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new ParallelFlatMap<>(this, mapper, false, maxConcurrency, prefetch));
    }

    public final <R> ParallelFolyam<R> flatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        return flatMapDelayError(mapper, FolyamPlugins.defaultBufferSize(), FolyamPlugins.defaultBufferSize());
    }

    public final <R> ParallelFolyam<R> flatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency) {
        return flatMapDelayError(mapper, maxConcurrency, FolyamPlugins.defaultBufferSize());
    }

    public final <R> ParallelFolyam<R> flatMapDelayError(CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
        Objects.requireNonNull(mapper, "mapper == null");
        return FolyamPlugins.onAssembly(new ParallelFlatMap<>(this, mapper, true, maxConcurrency, prefetch));
    }

    // rail-wise reductions

    public final <R> ParallelFolyam<R> reduce(Callable<? extends R> initialSupplier, CheckedBiFunction<R, ? super T, R> reducer) {
        Objects.requireNonNull(initialSupplier, "initialSupplier == null");
        Objects.requireNonNull(reducer, "reducer == null");
        return FolyamPlugins.onAssembly(new ParallelReduce<>(this, initialSupplier, reducer));
    }

    public final <R> ParallelFolyam<R> collect(Callable<? extends R> initialSupplier, CheckedBiConsumer<R, ? super T> collector) {
        Objects.requireNonNull(initialSupplier, "initialSupplier == null");
        Objects.requireNonNull(collector, "collector == null");
        return FolyamPlugins.onAssembly(new ParallelCollect<>(this, initialSupplier, collector));
    }

    // ------------------------------------------------------------------------
    // Converters back to Folyam/Esetleg
    // ------------------------------------------------------------------------

    public final Folyam<T> sequential() {
        return sequential(FolyamPlugins.defaultBufferSize());
    }

    public final Folyam<T> sequential(int prefetch) {
        return FolyamPlugins.onAssembly(new ParallelJoin<>(this, prefetch, false));
    }


    public final Folyam<T> sequentialDelayError() {
        return sequentialDelayError(FolyamPlugins.defaultBufferSize());
    }

    public final Folyam<T> sequentialDelayError(int prefetch) {
        return FolyamPlugins.onAssembly(new ParallelJoin<>(this, prefetch, true));
    }

    public final Folyam<T> sequential(SchedulerService executor) {
        return sequential(executor, FolyamPlugins.defaultBufferSize());
    }

    public final Folyam<T> sequential(SchedulerService executor, int prefetch) {
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> sequentialDelayError(SchedulerService executor) {
        return sequentialDelayError(executor, FolyamPlugins.defaultBufferSize());
    }

    public final Folyam<T> sequentialDelayError(SchedulerService executor, int prefetch) {
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> ignoreElements() {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> reduce(CheckedBiFunction<T, T, T> reducer) {
        Objects.requireNonNull(reducer, "reducer == null");
        return FolyamPlugins.onAssembly(new ParallelReduceFull<>(this, reducer));
    }

    public final Folyam<T> sorted(Comparator<? super T> comparator) {
        return sorted(comparator, 16);
    }

    public final Folyam<T> sorted(Comparator<? super T> comparator, int capacityHint) {
        Objects.requireNonNull(comparator, "comparator == null");
        int ch = capacityHint / parallelism() + 1;
        ParallelFolyam<List<T>> railReduced = reduce(() -> (List<T>)new ArrayList<T>(capacityHint), (a, b) -> { a.add(b); return a; });
        ParallelFolyam<List<T>> railSorted = railReduced.map(list -> { list.sort(comparator); return list; });

        return FolyamPlugins.onAssembly(new ParallelSortedJoin<T>(railSorted, comparator));
    }

    // aggregators

    public final Esetleg<T> min(Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator, "comparator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <K> Esetleg<T> min(CheckedFunction<? super T, ? extends K> keySelector, Comparator<? super K> comparator) {
        Objects.requireNonNull(keySelector, "keySelector == null");
        Objects.requireNonNull(comparator, "comparator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<T> max(Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator, "comparator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final <K> Esetleg<T> max(CheckedFunction<? super T, ? extends K> keySelector, Comparator<? super K> comparator) {
        Objects.requireNonNull(keySelector, "keySelector == null");
        Objects.requireNonNull(comparator, "comparator == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<Integer> sumInt(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<Long> sumLong(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<Float> sumFloat(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Esetleg<Double> sumDouble(CheckedFunction<? super T, ? extends Number> valueSelector) {
        Objects.requireNonNull(valueSelector, "valueSelector == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
