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
import hu.akarnokd.reactive4javaflow.impl.FailingFusedSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Flow;

public class FolyamZipArrayTest {

    @Test
    public void sameLength() {
        Folyam.zip(Folyam.range(1, 5), Folyam.range(1, 5), (a, b) -> a + "-" + b)
                .test()
                .assertResult("1-1", "2-2", "3-3", "4-4", "5-5");
    }

    @Test
    public void sameLengthHidden() {
        Folyam.zip(Folyam.range(1, 5).hide(), Folyam.range(1, 5).hide(), (a, b) -> a + "-" + b)
                .test()
                .assertResult("1-1", "2-2", "3-3", "4-4", "5-5");
    }

    @Test
    public void firstShorter() {
        Folyam.zip(Folyam.range(1, 4), Folyam.range(1, 5), (a, b) -> a + "-" + b)
                .test()
                .assertResult("1-1", "2-2", "3-3", "4-4");
    }

    @Test
    public void firstShorterHidden() {
        Folyam.zip(Folyam.range(1, 4).hide(), Folyam.range(1, 5).hide(), (a, b) -> a + "-" + b)
                .test()
                .assertResult("1-1", "2-2", "3-3", "4-4");
    }

    @Test
    public void secondShorter() {
        Folyam.zip(Folyam.range(1, 5), Folyam.range(1, 4), (a, b) -> a + "-" + b)
                .test()
                .assertResult("1-1", "2-2", "3-3", "4-4");
    }

    @Test
    public void secondShorterHidden() {
        Folyam.zip(Folyam.range(1, 5).hide(), Folyam.range(1, 4).hide(), (a, b) -> a + "-" + b)
                .test()
                .assertResult("1-1", "2-2", "3-3", "4-4");
    }

    @Test
    public void standardSameLength() {
        TestHelper.assertResult(Folyam.zip(Folyam.range(1, 5), Folyam.range(1, 5), (a, b) -> a + "-" + b),
                "1-1", "2-2", "3-3", "4-4", "5-5");
    }

    @Test
    public void standardSameLengthHidden() {
        TestHelper.assertResult(Folyam.zip(Folyam.range(1, 5).hide(), Folyam.range(1, 5).hide(), (a, b) -> a + "-" + b),
                "1-1", "2-2", "3-3", "4-4", "5-5");
    }

    @Test
    public void standardFirstShorter() {
        TestHelper.assertResult(Folyam.zip(Folyam.range(1, 4), Folyam.range(1, 5), (a, b) -> a + "-" + b),
                "1-1", "2-2", "3-3", "4-4");
    }

    @Test
    public void standardFirstShorterHidden() {
        TestHelper.assertResult(Folyam.zip(Folyam.range(1, 4).hide(), Folyam.range(1, 5).hide(), (a, b) -> a + "-" + b),
                "1-1", "2-2", "3-3", "4-4");
    }

    @Test
    public void standardSecondShorter() {
        TestHelper.assertResult(Folyam.zip(Folyam.range(1, 5), Folyam.range(1, 4), (a, b) -> a + "-" + b),
                "1-1", "2-2", "3-3", "4-4");
    }

    @Test
    public void standardSecondShorterHidden() {
        TestHelper.assertResult(Folyam.zip(Folyam.range(1, 5).hide(), Folyam.range(1, 4).hide(), (a, b) -> a + "-" + b),
                "1-1", "2-2", "3-3", "4-4");
    }

    @Test
    public void firstError() {
        Folyam.zip(Folyam.error(new IOException()), Folyam.range(1, 5), (a, b) -> a + "-" + b)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void firstErrorConditional() {
        Folyam.zip(Folyam.error(new IOException()), Folyam.range(1, 5), (a, b) -> a + "-" + b)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void secondError() {
        Folyam.zip(Folyam.range(1, 5), Folyam.error(new IOException()), (a, b) -> a + "-" + b)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void secondErrorConditional() {
        Folyam.zip(Folyam.range(1, 5), Folyam.error(new IOException()), (a, b) -> a + "-" + b)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void nondelayError() {
        Folyam.zip(Folyam.range(1, 5), Folyam.just(1).concatWith(Folyam.error(new IOException())), (a, b) -> a + "-" + b)
                .test()
                .assertFailure(IOException.class, "1-1");
    }

    @Test
    public void nondelayErrorConditional() {
        Folyam.zip(Folyam.range(1, 5), Folyam.just(1).concatWith(Folyam.error(new IOException())), (a, b) -> a + "-" + b)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, "1-1");
    }

    @Test
    public void delayError() {
        Folyam.zipDelayError(Folyam.just(1).concatWith(Folyam.error(new IOException())), Folyam.range(1, 5), (a, b) -> a + "-" + b)
                .test()
                .assertFailure(IOException.class, "1-1");
    }

    @Test
    public void delayErrorConditional() {
        Folyam.zipDelayError(Folyam.just(1).concatWith(Folyam.error(new IOException())), Folyam.range(1, 5), (a, b) -> a + "-" + b)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, "1-1");
    }

    @Test
    public void fusedCrash() {
        Folyam.zip(Folyam.just(1), new Folyam<Integer>() {

            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        }, (a, b) -> a + "-" + b)
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void fusedCrashConditional() {
        Folyam.zip(Folyam.just(1), new Folyam<Integer>() {

            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        }, (a, b) -> a + "-" + b)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void fusedCrashDelayError() {
        Folyam.zipDelayError(Folyam.just(1), new Folyam<Integer>() {

            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        }, (a, b) -> a + "-" + b)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void fusedCrashConditionalDelayError() {
        Folyam.zipDelayError(Folyam.just(1), new Folyam<Integer>() {

            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        }, (a, b) -> a + "-" + b)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void zipArray() {
        Flow.Publisher<Integer>[] sources = new Flow.Publisher[16];
        Arrays.fill(sources, Folyam.just(1));

        Folyam.zipArray(Arrays::toString, sources)
                .test()
                .assertResult("[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]");
    }


    @Test
    public void zipArrayDelayError() {
        Flow.Publisher<Integer>[] sources = new Flow.Publisher[16];
        Arrays.fill(sources, Folyam.just(1));

        Folyam.zipArrayDelayError(Arrays::toString, sources)
                .test()
                .assertResult("[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]");
    }

    @Test
    public void zipperCrash() {
        Folyam.zip(Folyam.just(1), Folyam.just(2), (a, b) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void zipperCrashConditional() {
        Folyam.zip(Folyam.just(1), Folyam.just(2), (a, b) -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void zipperCrashDelayError() {
        Folyam.zipDelayError(Folyam.just(1), Folyam.just(2), (a, b) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void zipperCrashConditionalDelayError() {
        Folyam.zipDelayError(Folyam.just(1), Folyam.just(2), (a, b) -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void zipLong() {
        Folyam.zip(Folyam.range(1, 1000), Folyam.range(1, 1000), (a, b) -> a + b)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void zipLongConditional() {
        Folyam.zip(Folyam.range(1, 1000), Folyam.range(1, 1000), (a, b) -> a + b)
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void zipLongDelayError() {
        Folyam.zipDelayError(Folyam.range(1, 1000), Folyam.range(1, 1000), (a, b) -> a + b)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void zipLongConditionalDelayError() {
        Folyam.zipDelayError(Folyam.range(1, 1000), Folyam.range(1, 1000), (a, b) -> a + b)
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void zipLongBy1() {
        Folyam.zip(Folyam.range(1, 1000), Folyam.range(1, 1000), (a, b) -> a + b, 1)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void zipLongConditionalBy1() {
        Folyam.zip(Folyam.range(1, 1000), Folyam.range(1, 1000), (a, b) -> a + b, 1)
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    public void zipLongBy1Hidden() {
        Folyam.zip(Folyam.range(1, 1000), Folyam.range(1, 1000).hide(), (a, b) -> a + b, 1)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void zipLongConditionalBy1Hidden() {
        Folyam.zip(Folyam.range(1, 1000), Folyam.range(1, 1000).hide(), (a, b) -> a + b, 1)
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void zipLongBy1DelayError() {
        Folyam.zipDelayError(Folyam.range(1, 1000), Folyam.range(1, 1000), (a, b) -> a + b, 1)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void zipLongConditionalBy1DelayError() {
        Folyam.zipDelayError(Folyam.range(1, 1000), Folyam.range(1, 1000), (a, b) -> a + b, 1)
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void zipArrayEmpty() {
        Folyam.zipArray(a -> a)
                .test()
                .assertResult();
    }

    @Test
    public void zipArrayOne() {
        Folyam.zipArray(a -> a[0], Folyam.range(1, 5))
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }


    @Test
    public void zipArrayOneConditional() {
        Folyam.zipArray(a -> a[0], Folyam.range(1, 5))
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void zipArrayFirstNull() {
        Folyam.zipArray(a -> a, null, Folyam.range(1, 5))
        .test()
        .assertFailure(NullPointerException.class);
    }

    @Test
    public void zipArrayFirstNullDelayError() {
        Folyam.zipArrayDelayError(a -> a, null,Folyam.range(1, 5))
                .test()
                .assertFailure(NullPointerException.class);
    }


    @Test
    public void zipArrayFirstNullConditional() {
        Folyam.zipArray(a -> a, null, Folyam.range(1, 5))
                .filter(v -> true)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void zipArrayFirstNullDelayErrorConditional() {
        Folyam.zipArrayDelayError(a -> a, null, Folyam.range(1, 5))
                .filter(v -> true)
                .test()
                .assertFailure(NullPointerException.class);
    }


    @Test
    public void zipArraySecondNull() {
        Folyam.zipArray(a -> a, Folyam.range(1, 5), null)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void zipArraySecondNullDelayError() {
        Folyam.zipArrayDelayError(a -> a, Folyam.range(1, 5), null)
                .test()
                .assertFailure(NullPointerException.class);
    }


    @Test
    public void zipArraySecondNullConditional() {
        Folyam.zipArray(a -> a, Folyam.range(1, 5), null)
                .filter(v -> true)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void zipArraySecondNullDelayErrorConditional() {
        Folyam.zipArrayDelayError(a -> a, Folyam.range(1, 5), null)
                .filter(v -> true)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void zipIterableMany() {
        List<Folyam<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(Folyam.just(1));
        }
        Folyam.zip(list, Arrays::toString)
                .test()
                .assertResult("[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]");
    }

    @Test
    public void zipDelayErrorIterableMany() {
        List<Folyam<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(Folyam.just(1));
        }
        Folyam.zipDelayError(list, Arrays::toString)
                .test()
                .assertResult("[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]");
    }


    @Test
    public void zipIterableManyByOne() {
        List<Folyam<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(Folyam.just(1));
        }
        Folyam.zip(list, Arrays::toString, 1)
                .test()
                .assertResult("[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]");
    }

    @Test
    public void zipDelayErrorIterableManyByOne() {
        List<Folyam<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(Folyam.just(1));
        }
        Folyam.zipDelayError(list, Arrays::toString, 1)
                .test()
                .assertResult("[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]");
    }

    @Test
    public void iterableFails() {
        Folyam.zip(new FailingMappedIterable<>(1, 10, 10, idx -> Folyam.never()), a -> a)
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void zipWith() {
        Folyam.range(1, 5).zipWith(Folyam.range(1, 6), (a, b) -> a + b)
                .test()
                .assertResult(2, 4, 6, 8, 10);
    }
}
