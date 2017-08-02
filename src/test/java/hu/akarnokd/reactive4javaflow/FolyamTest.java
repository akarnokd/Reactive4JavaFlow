/*
 * Copyright 2016-2017 David Karnok
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

import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.*;


public class FolyamTest {

    @Test
    public void helloWorld() {
        Folyam.just("Hello World!")
                .test()
                .assertResult("Hello World!");
    }

    void method(int paramName) {
        // deliberately empty
    }

    @Test
    public void javacParametersEnabled() throws Exception {
        assertEquals("Please enable saving parameter names via the -parameters javac argument",
                "paramName",
                getClass()
                .getDeclaredMethod("method", Integer.TYPE)
                .getParameters()[0].getName());
    }

    void checkFinalMethods(Class<?> clazz) {
        for (Method m : clazz.getMethods()) {

            if ((m.getModifiers() & (Modifier.STATIC | Modifier.ABSTRACT)) == 0
                    && m.getDeclaringClass() == clazz) {
                assertTrue(m.toString(), (m.getModifiers() & Modifier.FINAL) != 0);
            }

        }
    }

    @Test
    public void folyamFinalMethods() {
        checkFinalMethods(Folyam.class);
    }

    @Test
    public void esetlegFinalMethods() {
        checkFinalMethods(Esetleg.class);
    }

    @Test
    public void parallelFolyamFinalMethods() {
        checkFinalMethods(ParallelFolyam.class);
    }

    @Test
    public void connectableFolyamFinalMethods() {
        checkFinalMethods(ConnectableFolyam.class);
    }

    @Test
    public void backpressureModeEnum() {
        TestHelper.checkEnum(BackpressureHandling.class);
    }

    @Test
    public void compose() {
        Folyam.range(1, 5)
                .compose(f -> f.map(g -> g + 1))
                .test()
                .assertResult(2, 3, 4, 5, 6);
    }

    @Test
    public void subscribe0() {
        Folyam.range(1, 5)
                .subscribe();
    }

    @Test
    public void subscribe0Error() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.error(new IOException("failure"))
                    .subscribe();

            TestHelper.assertError(errors, 0, IOException.class, "failure");
        });
    }

    @Test
    public void subscribe1() {
        List<Integer> items = new ArrayList<>();
        Folyam.range(1, 5)
                .subscribe(items::add);

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), items);
    }

    @Test
    public void subscribe1Error() {
        TestHelper.withErrorTracking(errors -> {
            List<Integer> items = new ArrayList<>();
            Folyam.<Integer>error(new IOException("failure"))
                    .subscribe(items::add);

            TestHelper.assertError(errors, 0, IOException.class, "failure");
        });
    }

    @Test
    public void subscribe2() {
        List<Integer> items = new ArrayList<>();
        Folyam.range(1, 5)
                .subscribe(items::add, e -> items.add(100));

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), items);
    }

    @Test
    public void subscribe3() {
        List<Integer> items = new ArrayList<>();
        Folyam.range(1, 5)
                .subscribe(items::add, e -> items.add(100), () -> items.add(200));

        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 200), items);
    }

    @Test
    public void subscribeWith() {
        Folyam.range(1, 5)
                .subscribeWith(new TestConsumer<>())
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void folyamCrash() {
        TestHelper.withErrorTracking(errors -> {
            new Folyam<Integer>() {
                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new BooleanSubscription());
                    throw new IllegalArgumentException("failure");
                }
            }
            .test()
            .assertEmpty()
            ;
            TestHelper.assertError(errors, 0, IllegalArgumentException.class, "failure");
        });
    }

    @Test
    public void runTestNoCancel() {
        Folyam.just(1)
                .test(1, false, 0)
                .assertResult(1);
    }

    @Test
    public void runTestCancel() {
        Folyam.just(1)
                .test(1, true, 0)
                .assertEmpty();
    }

    @Test
    public void toList() {
        Folyam.range(1, 5).toList()
                .test()
                .assertResult(List.of(1, 2, 3, 4, 5));
    }

    @Test
    public void fromStream() {
        Folyam<Integer> f = Folyam.fromStream(IntStream.range(1, 6).boxed());

        f.test().assertResult(1, 2, 3, 4, 5);

        f.test().assertFailure(IllegalStateException.class);
    }

    @Test
    public void fromOptional() {
        Folyam.fromOptional(Optional.of(1))
                .test()
                .assertResult(1);
    }

    @Test
    public void fromOptionalEmpty() {
        Folyam.fromOptional(Optional.empty())
                .test()
                .assertResult();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void fusedOfferTrows() {
        new FusedSubscription<>() {

            @Override
            public void request(long n) {

            }

            @Override
            public void cancel() {

            }

            @Override
            public Object poll() throws Throwable {
                return null;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public void clear() {

            }

            @Override
            public int requestFusion(int mode) {
                return 0;
            }
        }.offer(1);
    }

    @Test
    public void fromPublisherAlreadyAFolyam() {
        Folyam<Integer> f = Folyam.just(1);

        assertSame(f, Folyam.fromPublisher(f));
    }


    @Test
    public void fromPublisher() {
        Folyam<Integer> f = Folyam.just(1);

        Folyam.fromPublisher(f::subscribe)
                .test()
                .assertResult(1);
    }
}
