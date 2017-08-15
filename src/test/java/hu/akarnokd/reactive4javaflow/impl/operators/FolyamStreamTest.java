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

package hu.akarnokd.reactive4javaflow.impl.operators;

import hu.akarnokd.reactive4javaflow.*;
import org.junit.Test;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class FolyamStreamTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.defer(() ->
                    Folyam.fromStream(List.of(1, 2, 3, 4, 5).stream(), true)),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standardConditional() {
        TestHelper.assertResult(
                Folyam.defer(() ->
                                Folyam.fromStream(List.of(1, 2, 3, 4, 5).stream(), true)
                    .filter(v -> true)),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.defer(() -> Folyam.fromStream(List.of(1).stream(), true)),
                1);
    }


    @Test
    public void standard1Conditional() {
        TestHelper.assertResult(
                Folyam.defer(() ->Folyam.fromStream(List.of(1).stream(), true))
                        .filter(v -> true),
                1);
    }

    @Test
    public void empty() {
        Folyam.fromStream(List.of().stream(), true)
                .test()
                .assertResult();
    }

    @Test
    public void hasNextInitialFail() {
        Folyam.fromStream(toStream(new FailingIterable(10, 1, 10)), true)
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "hasNext");
    }

    @Test
    public void hasNextFail() {
        Folyam.fromStream(toStream(new FailingIterable(10, 2, 10)), true)
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "hasNext", 1);
    }


    @Test
    public void hasNextFailConditional() {
        Folyam.fromStream(toStream(new FailingIterable(10, 2, 10)), true)
                .filter(v -> true)
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "hasNext", 1);
    }


    @Test
    public void hasNextFailBackpressured() {
        Folyam.fromStream(toStream(new FailingIterable(10, 2, 10)), true)
                .test(2)
                .assertFailureAndMessage(IllegalStateException.class, "hasNext", 1);
    }


    @Test
    public void hasNextFailConditionalBackpressured() {
        Folyam.fromStream(toStream(new FailingIterable(10, 2, 10)), true)
                .filter(v -> true)
                .test(2)
                .assertFailureAndMessage(IllegalStateException.class, "hasNext", 1);
    }

    @Test
    public void nextFail() {
        Folyam.fromStream(toStream(new FailingIterable(10, 10, 1)), true)
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "next");
    }

    @Test
    public void nextFailConditional() {
        Folyam.fromStream(toStream(new FailingIterable(10, 10, 1)), true)
                .filter(v -> true)
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "next");
    }

    @Test
    public void nextFailBackpressured() {
        Folyam.fromStream(toStream(new FailingIterable(10, 10, 1)), true)
                .test(2)
                .assertFailureAndMessage(IllegalStateException.class, "next");
    }

    @Test
    public void nextFailConditionalBackpressured() {
        Folyam.fromStream(toStream(new FailingIterable(10, 10, 1)), true)
                .filter(v -> true)
                .test(2)
                .assertFailureAndMessage(IllegalStateException.class, "next");
    }

    static <T> Stream<T> toStream(Iterable<T> iter) {
        return new Stream<T>() {
            @Override
            public Stream<T> filter(Predicate<? super T> predicate) {
                return null;
            }

            @Override
            public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
                return null;
            }

            @Override
            public IntStream mapToInt(ToIntFunction<? super T> mapper) {
                return null;
            }

            @Override
            public LongStream mapToLong(ToLongFunction<? super T> mapper) {
                return null;
            }

            @Override
            public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
                return null;
            }

            @Override
            public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
                return null;
            }

            @Override
            public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
                return null;
            }

            @Override
            public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
                return null;
            }

            @Override
            public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
                return null;
            }

            @Override
            public Stream<T> distinct() {
                return null;
            }

            @Override
            public Stream<T> sorted() {
                return null;
            }

            @Override
            public Stream<T> sorted(Comparator<? super T> comparator) {
                return null;
            }

            @Override
            public Stream<T> peek(Consumer<? super T> action) {
                return null;
            }

            @Override
            public Stream<T> limit(long maxSize) {
                return null;
            }

            @Override
            public Stream<T> skip(long n) {
                return null;
            }

            @Override
            public void forEach(Consumer<? super T> action) {

            }

            @Override
            public void forEachOrdered(Consumer<? super T> action) {

            }

            @Override
            public Object[] toArray() {
                return new Object[0];
            }

            @Override
            public <A> A[] toArray(IntFunction<A[]> generator) {
                return null;
            }

            @Override
            public T reduce(T identity, BinaryOperator<T> accumulator) {
                return null;
            }

            @Override
            public Optional<T> reduce(BinaryOperator<T> accumulator) {
                return Optional.empty();
            }

            @Override
            public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
                return null;
            }

            @Override
            public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
                return null;
            }

            @Override
            public <R, A> R collect(Collector<? super T, A, R> collector) {
                return null;
            }

            @Override
            public Optional<T> min(Comparator<? super T> comparator) {
                return Optional.empty();
            }

            @Override
            public Optional<T> max(Comparator<? super T> comparator) {
                return Optional.empty();
            }

            @Override
            public long count() {
                return 0;
            }

            @Override
            public boolean anyMatch(Predicate<? super T> predicate) {
                return false;
            }

            @Override
            public boolean allMatch(Predicate<? super T> predicate) {
                return false;
            }

            @Override
            public boolean noneMatch(Predicate<? super T> predicate) {
                return false;
            }

            @Override
            public Optional<T> findFirst() {
                return Optional.empty();
            }

            @Override
            public Optional<T> findAny() {
                return Optional.empty();
            }

            @Override
            public Iterator<T> iterator() {
                return iter.iterator();
            }

            @Override
            public Spliterator<T> spliterator() {
                return Spliterators.spliterator(iterator(), 0, 0);
            }

            @Override
            public boolean isParallel() {
                return false;
            }

            @Override
            public Stream<T> sequential() {
                return this;
            }

            @Override
            public Stream<T> parallel() {
                return this;
            }

            @Override
            public Stream<T> unordered() {
                return this;
            }

            @Override
            public Stream<T> onClose(Runnable closeHandler) {
                return this;
            }

            @Override
            public void close() {

            }
        };
    }

}
