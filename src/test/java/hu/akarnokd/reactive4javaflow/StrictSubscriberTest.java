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
import java.util.concurrent.Flow;

import static org.junit.Assert.assertTrue;

public class StrictSubscriberTest {

    @Test
    public void error() {
        TestConsumer<Integer> ts = new TestConsumer<>();

        Folyam.<Integer>error(new IOException())
            .subscribe(new Flow.Subscriber<>() {

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    ts.onSubscribe(new BooleanSubscription());
                    subscription.request(Long.MAX_VALUE);
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

        ts.assertFailure(IOException.class);
    }

    @Test
    public void badRequest() {
        TestHelper.withErrorTracking(errors -> {
            TestConsumer<Integer> ts = new TestConsumer<>();

            Folyam.just(1)
                    .subscribe(new Flow.Subscriber<>() {

                        @Override
                        public void onSubscribe(Flow.Subscription subscription) {
                            ts.onSubscribe(new BooleanSubscription());
                            subscription.request(-1L);
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

            ts.assertFailure(IllegalArgumentException.class);

            assertTrue(errors.toString(), errors.isEmpty());
        });
    }
}
