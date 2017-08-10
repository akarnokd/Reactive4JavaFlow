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
import java.util.concurrent.TimeUnit;

public class FolyamRetryWhenTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .retryWhen(h -> h),
                1, 2, 3, 4, 5
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .retryWhen(h -> h),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.defer(() -> {
                    int[] c = { 0 };
                    return Folyam.defer(() -> {
                        Folyam<Integer> f = Folyam.range(1, 5);
                        if (c[0]++ < 2) {
                            f = f.concatWith(Folyam.error(new IOException()));
                        }
                        return f;
                    })
                    .retryWhen(h -> h);
                }),
                1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5
        );
    }

    @Test
    public void becomeEmpty() {
        TestHelper.assertResult(
                Folyam.error(new IOException()).retryWhen(h -> {
                    int[] c = { 0 };
                    return h.takeWhile(v -> c[0]++ < 2);
                }));
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.retryWhen(h -> {
                    int[] c = { 0 };
                    return h.takeWhile(v -> {
                        if (c[0]++ < 2) {
                            return true;
                        }
                        throw v;
                    });
                }), IOException.class);
    }

    @Test
    public void retryPass2() {
        TestConsumer<Integer> ts = new TestConsumer<>(0);

        int[] c = { 0 };

        Folyam.defer(() -> {
            Folyam<Integer> f = Folyam.range(1, 5);
            if (c[0]++ < 2) {
                f = f.concatWith(Folyam.error(new IOException()));
            }
            return f;
        })
                .retryWhen(h -> h)
                .subscribe(ts);

        Integer[] values = { 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5 };
        for (int i = 0; i < values.length; i++) {
            ts.requestMore(1)
                    .awaitCount(i + 1, 10, 5000)
                    .assertValueCount(i + 1)
            ;
        }

        ts.awaitDone(5, TimeUnit.SECONDS)
                .assertResult(values);
    }

    @Test
    public void handlerCrash() {
        Folyam.range(1, 5)
                .retryWhen(h -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void immediateError() {
        TestHelper.assertFailureComposed(5,
                f -> f.retryWhen(v -> Folyam.error(new IOException())), IOException.class);
    }

    @Test
    public void immediateComplete() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .retryWhen(h -> Folyam.empty())
        );
    }

    @Test
    public void unrelated() {
        Folyam.range(1, 5).concatWith(Folyam.error(new IOException()))
                .retryWhen(h -> Folyam.range(1, 2))
                .test()
                .assertResult(1, 2, 3, 4, 5, 1, 2, 3, 4, 5);
    }


    @Test
    public void unrelatedConditional() {
        Folyam.range(1, 5).concatWith(Folyam.error(new IOException()))
                .retryWhen(h -> Folyam.range(1, 2))
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5, 1, 2, 3, 4, 5);
    }

    @Test
    public void repeatAsyncSource() {
        Folyam.range(1, 5).concatWith(Folyam.error(new IOException()))
                .subscribeOn(SchedulerServices.single())
                .retryWhen(h -> {
                    int[] c = { 0 };
                    return h.takeWhile(v -> c[0]++ < 100);
                })
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertValueCount(505)
                .assertNoErrors()
                .assertComplete();
    }

}
