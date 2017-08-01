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

import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;

public class AbstractDisposableSubscriberTest {

    @Test
    public void normal() {
        TestConsumer<Integer> ts = new TestConsumer<>();
        ts.onSubscribe(new BooleanSubscription());
        Folyam.range(1, 5)
                .subscribe(new AbstractDisposableSubscriber<Integer>() {
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

        ts.assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void oneByOne() {
        TestConsumer<Integer> ts = new TestConsumer<>();
        ts.onSubscribe(new BooleanSubscription());
        Folyam.range(1, 5)
                .subscribe(new AbstractDisposableSubscriber<Integer>() {
                    @Override
                    protected void onStart() {
                        request(1);
                    }

                    @Override
                    public void onNext(Integer item) {
                        ts.onNext(item);
                        request(1);
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

        ts.assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void cancelAfter3() {
        TestConsumer<Integer> ts = new TestConsumer<>();
        ts.onSubscribe(new BooleanSubscription());
        Folyam.range(1, 5)
                .subscribe(new AbstractDisposableSubscriber<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        ts.onNext(item);
                        if (item == 3) {
                            close();
                            ts.onComplete();
                        }
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

        ts.assertResult(1, 2, 3);
    }

    @Test
    public void error() {
        TestConsumer<Integer> ts = new TestConsumer<>();
        ts.onSubscribe(new BooleanSubscription());
        Folyam.<Integer>error(new IOException())
                .subscribe(new AbstractDisposableSubscriber<Integer>() {
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

        ts.assertFailure(IOException.class);
    }

    @Test
    public void negativeRequest() {
        TestHelper.withErrorTracking(errors -> {
            TestConsumer<Integer> ts = new TestConsumer<>();
            ts.onSubscribe(new BooleanSubscription());
            Folyam.range(1, 5)
                    .subscribe(new AbstractDisposableSubscriber<Integer>() {
                        @Override
                        protected void onStart() {
                            request(-1);
                        }

                        @Override
                        public void onNext(Integer item) {
                            ts.onNext(item);
                            request(1);
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

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }
}
