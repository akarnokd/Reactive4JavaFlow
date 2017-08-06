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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Flow;

public class FolyamIgnoreElementsTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.just(1).ignoreElements());
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .ignoreElements()
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void fused() {
        TestConsumer<Integer> ts = new TestConsumer<>();

        Folyam.just(1)
                .ignoreElements()
                .subscribe(new FolyamSubscriber<Integer>() {
                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        ts.onSubscribe(new BooleanSubscription());
                        FusedSubscription<Integer> fs = (FusedSubscription<Integer>)subscription;

                        int m = fs.requestFusion(FusedSubscription.ANY);
                        if (m != FusedSubscription.ASYNC) {
                            ts.onError(new AssertionError("Wrong fusion mode: " + m));
                        }
                        if (!fs.isEmpty()) {
                            ts.onError(new AssertionError("Not empty"));
                        }
                        Integer v;

                        try {
                            v = fs.poll();
                        } catch (Throwable ex) {
                            ts.onError(ex);
                            v = null;
                        }
                        if (v != null) {
                            ts.onNext(v);
                        }

                        fs.clear();

                        if (!fs.isEmpty()) {
                            ts.onError(new AssertionError("Not empty"));
                        }

                        try {
                            v = fs.poll();
                        } catch (Throwable ex) {
                            ts.onError(ex);
                            v = null;
                        }
                        if (v != null) {
                            ts.onNext(v);
                        }

                        fs.cancel();

                        if (!fs.isEmpty()) {
                            ts.onError(new AssertionError("Not empty"));
                        }

                        try {
                            v = fs.poll();
                        } catch (Throwable ex) {
                            ts.onError(ex);
                            v = null;
                        }
                        if (v != null) {
                            ts.onNext(v);
                        }
                    }

                    @Override
                    public void onNext(Integer item) {
                        ts.onNext(item);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        ts.onError(throwable);
                    }

                    @Override
                    public void onComplete() {
                        ts.onComplete();
                    }
                });

        ts.assertEmpty();
    }
}
