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

import java.io.IOException;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.util.stream.Collector.Characteristics.UNORDERED;

public class FolyamStreamCollectorTest {

    @Test
    public void standard() {
        TestHelper.<List<Integer>>assertResult(
                Folyam.range(1, 5)
                .collect(Collectors.toList()),
                Arrays.asList(1, 2, 3, 4, 5)
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .collect(Collectors.toList())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void supplierCrash() {
        Folyam.range(1, 5)
                .collect(new Collector<Integer, Integer, Integer>() {

                    @Override
                    public Supplier<Integer> supplier() {
                        return () -> { throw new IllegalArgumentException(); };
                    }

                    @Override
                    public BiConsumer<Integer, Integer> accumulator() {
                        return (a, b) -> { };
                    }

                    @Override
                    public BinaryOperator<Integer> combiner() {
                        return (a, b) -> a;
                    }

                    @Override
                    public Function<Integer, Integer> finisher() {
                        return v -> v;
                    }

                    @Override
                    public Set<Characteristics> characteristics() {
                        return Set.of(UNORDERED);
                    }
                })
                .test()
                .assertFailure(IllegalArgumentException.class);
    }

    @Test
    public void accumulatorCrash() {
        Folyam.range(1, 5)
                .collect(new Collector<Integer, Integer, Integer>() {

                    @Override
                    public Supplier<Integer> supplier() {
                        return () -> 1;
                    }

                    @Override
                    public BiConsumer<Integer, Integer> accumulator() {
                        return (a, b) -> { throw new IllegalArgumentException(); };
                    }

                    @Override
                    public BinaryOperator<Integer> combiner() {
                        return (a, b) -> a;
                    }

                    @Override
                    public Function<Integer, Integer> finisher() {
                        return v -> v;
                    }

                    @Override
                    public Set<Characteristics> characteristics() {
                        return Set.of(UNORDERED);
                    }
                })
                .test()
                .assertFailure(IllegalArgumentException.class);
    }

    @Test
    public void finisherCrash() {
        Folyam.range(1, 5)
                .collect(new Collector<Integer, Integer, Integer>() {

                    @Override
                    public Supplier<Integer> supplier() {
                        return () -> 1;
                    }

                    @Override
                    public BiConsumer<Integer, Integer> accumulator() {
                        return (a, b) -> {  };
                    }

                    @Override
                    public BinaryOperator<Integer> combiner() {
                        return (a, b) -> a;
                    }

                    @Override
                    public Function<Integer, Integer> finisher() {
                        return v -> { throw new IllegalArgumentException(); };
                    }

                    @Override
                    public Set<Characteristics> characteristics() {
                        return Set.of(UNORDERED);
                    }
                })
                .test()
                .assertFailure(IllegalArgumentException.class);
    }

    @Test
    public void donePath() {
        TestHelper.<List<Integer>>folyamDonePath(f -> f.collect(Collectors.toList()), List.of(1));
    }
}
