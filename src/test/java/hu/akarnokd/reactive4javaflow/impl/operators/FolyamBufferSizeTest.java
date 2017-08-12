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
import java.util.List;

public class FolyamBufferSizeTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 10).buffer(2),
                List.of(1, 2), List.of(3, 4), List.of(5, 6), List.of(7, 8), List.of(9, 10)
        );
    }


    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.range(1, 9).buffer(2),
                List.of(1, 2), List.of(3, 4), List.of(5, 6), List.of(7, 8), List.of(9)
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.empty().buffer(2)
        );
    }

    @Test
    public void normalSkip() {
        Folyam.range(1, 10).buffer(1, 2)
        .test()
        .assertResult(List.of(1), List.of(3), List.of(5), List.of(7), List.of(9));
    }


    @Test
    public void normalSkipBackpressured() {
        TestConsumer<List<Integer>> tc = Folyam.range(1, 10).buffer(1, 2)
                .test(0L);

        tc.assertEmpty()
                .requestMore(1)
                .assertValues(List.of(1))
                .requestMore(2)
                .assertValues(List.of(1), List.of(3), List.of(5))
                .requestMore(3)
                .assertResult(List.of(1), List.of(3), List.of(5), List.of(7), List.of(9));
    }

    @Test
    public void standard4() {
        Folyam.range(1, 10).buffer(2, 3)
        .test()
        .assertResult(List.of(1, 2), List.of(4, 5), List.of(7, 8), List.of(10));
    }

    @Test
    public void normalOverlap() {
        Folyam.range(1, 10)
                .buffer(2, 1)
                .test()
                .assertResult(
                    List.of(1, 2), List.of(2, 3), List.of(3, 4),
                    List.of(4, 5), List.of(5, 6), List.of(6, 7),
                    List.of(7, 8), List.of(8, 9), List.of(9, 10),
                    List.of(10)
                );
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(Folyam.range(1, 10)
                .buffer(2, 1),
                List.of(1, 2), List.of(2, 3), List.of(3, 4),
                List.of(4, 5), List.of(5, 6), List.of(6, 7),
                List.of(7, 8), List.of(8, 9), List.of(9, 10),
                List.of(10)
                );

    }

    @Test
    public void standard6() {
        TestHelper.assertResult(Folyam.range(1, 10)
                        .buffer(3, 1),
                List.of(1, 2, 3), List.of(2, 3, 4), List.of(3, 4, 5),
                List.of(4, 5, 6), List.of(5, 6, 7), List.of(6, 7, 8),
                List.of(7, 8, 9), List.of(8, 9, 10), List.of(9, 10),
                List.of(10)
        );
    }


    @Test
    public void standard7() {
        TestHelper.assertResult(Folyam.range(1, 10)
                        .buffer(3, 2),
                List.of(1, 2, 3), List.of(3, 4, 5), List.of(5, 6, 7),
                List.of(7, 8, 9), List.of(9, 10)
        );
    }

    @Test
    public void overlapRequest0() {
        Folyam.range(1, 10)
                .buffer(3, 2)
                .test(0)
                .assertEmpty()
                .requestMore(5)
                .assertResult(
                        List.of(1, 2, 3), List.of(3, 4, 5), List.of(5, 6, 7),
                        List.of(7, 8, 9), List.of(9, 10)
                );
    }

    @Test
    public void errorExact() {
        Folyam.error(new IOException())
                .buffer(2)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorSkip() {
        Folyam.error(new IOException())
                .buffer(1, 2)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorOverlap() {
        Folyam.error(new IOException())
                .buffer(2, 1)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void supplierExactCrash() {
        Folyam.range(1, 5)
                .buffer(1, 1, () -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void supplierSkipCrash() {
        Folyam.range(1, 5)
                .buffer(1, 2, () -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void supplierOverlapCrash() {
        Folyam.range(1, 5)
                .buffer(2, 1, () -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void take() {
        Folyam.range(1, 5)
                .buffer(1, 2)
                .take(2)
                .test()
                .assertResult(List.of(1), List.of(3));

    }

    @Test
    public void badSourceOverlap() {
        TestHelper.withErrorTracking(errors -> {
            new Folyam<Integer>() {
                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new BooleanSubscription());
                    s.onError(new IOException());
                    s.onNext(1);
                    s.onError(new IllegalArgumentException());
                    s.onComplete();
                }
            }
                    .buffer(3, 1)
                    .test()
                    .assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }
}
