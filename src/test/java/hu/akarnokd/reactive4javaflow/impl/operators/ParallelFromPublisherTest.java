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

import static org.junit.Assert.*;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.hot.SolocastProcessor;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;

public class ParallelFromPublisherTest {

    @Test
    public void sourceOverflow() {
        new Folyam<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new BooleanSubscription());
                for (int i = 0; i < 10; i++) {
                    s.onNext(i);
                }
            }
        }
        .parallel(1, 1)
        .sequential(1)
        .test(0)
        .assertFailure(IllegalStateException.class);
    }

    @Test
    public void fusedFilterBecomesEmpty() {
        Folyam.just(1)
        .filter(v -> false)
        .parallel()
        .sequential()
        .test()
        .assertResult();
    }

    @Test
    public void syncFusedMapCrash() {
        Folyam.just(1)
        .map(new CheckedFunction<Integer, Object>() {
            @Override
            public Object apply(Integer v) throws Exception {
                throw new IOException();
            }
        })
        .parallel()
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void asyncFusedMapCrash() {
        SolocastProcessor<Integer> up = new SolocastProcessor<>();

        up.onNext(1);

        up
        .map(new CheckedFunction<Integer, Object>() {
            @Override
            public Object apply(Integer v) throws Exception {
                throw new IOException();
            }
        })
        .parallel()
        .sequential()
        .test()
        .assertFailure(IOException.class);

        assertFalse(up.hasSubscribers());
    }
}
