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
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

public class FolyamBufferPredicateTest {

    @SuppressWarnings("unchecked")
    @Test
    public void whileNormal() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferWhile(v -> v != -1)
                .test()
                .assertResult(
                        Arrays.asList(1, 2),
                        Arrays.asList(-1, 3, 4, 5),
                        Collections.singletonList(-1),
                        Arrays.asList(-1, 6)
                );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void whileNormalHidden() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6).hide()
                .bufferWhile(v -> v != -1)
                .test()
                .assertResult(
                        Arrays.asList(1, 2),
                        Arrays.asList(-1, 3, 4, 5),
                        Collections.singletonList(-1),
                        Arrays.asList(-1, 6)
                );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void whileNormalBackpressure() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferWhile(v -> v != -1)
                .test(0)
                .assertValueCount(0)
                .requestMore(1)
                .assertValues(Arrays.asList(1, 2))
                .requestMore(2)
                .assertValues(Arrays.asList(1, 2),
                        Arrays.asList(-1, 3, 4, 5),
                        Collections.singletonList(-1))
                .requestMore(1)
                .assertResult(
                        Arrays.asList(1, 2),
                        Arrays.asList(-1, 3, 4, 5),
                        Collections.singletonList(-1),
                        Arrays.asList(-1, 6)
                );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void untilNormal() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferUntil(v -> v == -1)
                .test()
                .assertResult(
                        Arrays.asList(1, 2, -1),
                        Arrays.asList(3, 4, 5, -1),
                        Collections.singletonList(-1),
                        Collections.singletonList(6)
                );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void untilNormalHidden() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6).hide()
                .bufferUntil(v -> v == -1)
                .test()
                .assertResult(
                        Arrays.asList(1, 2, -1),
                        Arrays.asList(3, 4, 5, -1),
                        Collections.singletonList(-1),
                        Collections.singletonList(6)
                );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void untilNormalBackpressured() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferUntil(v -> v == -1)
                .test(0)
                .assertValueCount(0)
                .requestMore(1)
                .assertValues(Arrays.asList(1, 2, -1))
                .requestMore(2)
                .assertValues(Arrays.asList(1, 2, -1),
                        Arrays.asList(3, 4, 5, -1),
                        Collections.singletonList(-1))
                .requestMore(1)
                .assertResult(
                        Arrays.asList(1, 2, -1),
                        Arrays.asList(3, 4, 5, -1),
                        Collections.singletonList(-1),
                        Collections.singletonList(6)
                );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void emptyWhile() {
        Folyam.<Integer>empty()
                .bufferWhile(v -> v != -1)
                .test()
                .assertResult();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void emptyUntil() {
        Folyam.<Integer>empty()
                .bufferUntil(v -> v == -1)
                .test()
                .assertResult();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void errorWhile() {
        Folyam.<Integer>error(new IOException())
                .bufferWhile(v -> v != -1)
                .test()
                .assertFailure(IOException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void errorUntil() {
        Folyam.<Integer>error(new IOException())
                .bufferUntil(v -> v == -1)
                .test()
                .assertFailure(IOException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void whileTake() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferWhile(v -> v != -1)
                .take(2)
                .test()
                .assertResult(
                        Arrays.asList(1, 2),
                        Arrays.asList(-1, 3, 4, 5)
                );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void untilTake() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferUntil(v -> v == -1)
                .take(2)
                .test()
                .assertResult(
                        Arrays.asList(1, 2, -1),
                        Arrays.asList(3, 4, 5, -1)
                );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void CheckedPredicateCrash() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferUntil(v -> {
                    throw new IllegalArgumentException();
                })
                .test()
                .assertFailure(IllegalArgumentException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void bufferSupplierCrash0() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferUntil(v -> v == -1, (Callable<List<Integer>>) () -> {
                    throw new IllegalArgumentException();
                })
                .test()
                .assertFailure(IllegalArgumentException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void bufferSupplierCrash1() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferUntil(v -> v == -1, new Callable<List<Integer>>() {
                    int c;
                    @Override
                    public List<Integer> call() throws Exception {
                        if (c++ == 1) {
                            throw new IllegalArgumentException();
                        }
                        return new ArrayList<>();
                    }
                })
                .test()
                .assertFailure(IllegalArgumentException.class, Arrays.asList(1, 2, -1));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void bufferSupplierCrash2() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferWhile(v -> v != -1, new Callable<List<Integer>>() {
                    int c;
                    @Override
                    public List<Integer> call() throws Exception {
                        if (c++ == 1) {
                            throw new IllegalArgumentException();
                        }
                        return new ArrayList<>();
                    }
                })
                .test()
                .assertFailure(IllegalArgumentException.class, Arrays.asList(1, 2));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void bufferSupplierCrash3() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferSplit(v -> v == -1, new Callable<List<Integer>>() {
                    int c;
                    @Override
                    public List<Integer> call() throws Exception {
                        if (c++ == 1) {
                            throw new IllegalArgumentException();
                        }
                        return new ArrayList<>();
                    }
                })
                .test()
                .assertFailure(IllegalArgumentException.class, Arrays.asList(1, 2));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doubleError() {
        TestHelper.withErrorTracking(errors -> {
            new Folyam<Integer>() {
                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new BooleanSubscription());
                    s.onError(new IllegalArgumentException());
                    s.onError(new IOException());
                }
            }
                    .bufferWhile(v -> v != -1)
                    .test()
                    .assertFailure(IllegalArgumentException.class);

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void splitNormal() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferSplit(v -> v == -1)
                .test()
                .assertResult(
                        Arrays.asList(1, 2),
                        Arrays.asList(3, 4, 5),
                        Collections.emptyList(),
                        Collections.singletonList(6)
                );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void splitNormalHidden() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6).hide()
                .bufferSplit(v -> v == -1)
                .test()
                .assertResult(
                        Arrays.asList(1, 2),
                        Arrays.asList(3, 4, 5),
                        Collections.emptyList(),
                        Collections.singletonList(6)
                );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void splitNormalBackpressure() {
        Folyam.fromArray(1, 2, -1, 3, 4, 5, -1, -1, 6)
                .bufferSplit(v -> v == -1)
                .test(0)
                .assertValueCount(0)
                .requestMore(1)
                .assertValues(Arrays.asList(1, 2))
                .requestMore(2)
                .assertValues(Arrays.asList(1, 2),
                        Arrays.asList(3, 4, 5),
                        Collections.emptyList())
                .requestMore(1)
                .assertResult(
                        Arrays.asList(1, 2),
                        Arrays.asList(3, 4, 5),
                        Collections.emptyList(),
                        Collections.singletonList(6)
                );
    }
}
