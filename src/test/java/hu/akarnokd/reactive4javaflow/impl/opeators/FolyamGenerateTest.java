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

package hu.akarnokd.reactive4javaflow.impl.opeators;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import org.junit.Test;

import java.io.IOException;

public class FolyamGenerateTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.generate(() -> 0, (s, e) -> {
            e.onNext(s);
            if (s == 4) {
                e.onComplete();
            }
            return s + 1;
        }, s -> { }), 0, 1, 2, 3, 4);
    }

    @Test
    public void nullItem() {
        Folyam.generate(e -> { e.onNext(null);})
        .test()
        .assertFailureAndMessage(NullPointerException.class, "item == null");
    }

    @Test
    public void nullError() {
        Folyam.generate(e -> { e.onError(null);})
                .test()
                .assertFailureAndMessage(NullPointerException.class, "ex == null");
    }

    @Test
    public void generatorCrash() {
        Folyam.generate(e -> { throw new UnsupportedOperationException(); })
                .test()
                .assertFailure(UnsupportedOperationException.class);
    }


    @Test
    public void cleanupCrash() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.generate(() -> 0, (s, e) -> { e.onComplete(); }, s -> { throw new IOException(); })
                    .test()
                    .assertResult();

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void doubleOnNext() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.generate(e -> {
                e.onNext(1);
                e.onNext(2);
                e.onError(new IOException());
            })
            .test()
            .assertFailure(IllegalStateException.class, 1);

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void stateSupplierCrash() {
        Folyam.generate(() -> { throw new IOException(); }, (s, e) -> s)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void generatorErrorAndCrash() {
        Folyam.generate(e -> {
            e.onError(new IOException());
            throw new IllegalArgumentException();
        })
        .test()
        .assertFailure(CompositeThrowable.class)
        .assertInnerErrors(errors -> {
            TestHelper.assertError(errors, 0, IOException.class);
            TestHelper.assertError(errors, 1, IllegalArgumentException.class);
        });
    }

    @Test
    public void fusedValueAndError() {
        Folyam.generate(e -> {
            e.onNext(1);
            e.onError(new IOException());
        })
        .test(0L, false, FusedSubscription.ANY)
        .assertFusionMode(FusedSubscription.SYNC)
        .assertFailure(IOException.class, 1)
        ;
    }

    @Test
    public void fusedError() {
        Folyam.generate(e -> {
            e.onError(new IOException());
        })
                .test(0L, false, FusedSubscription.ANY)
                .assertFusionMode(FusedSubscription.SYNC)
                .assertFailure(IOException.class)
        ;
    }

    @Test
    public void noMethodCall() {
        Folyam.generate(e -> { })
        .test()
        .assertFailure(IllegalStateException.class);
    }

    @Test
    public void noMethodCallFused() {
        Folyam.generate(e -> { })
                .test(0L, false, FusedSubscription.ANY)
                .assertFusionMode(FusedSubscription.SYNC)
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void boundaryNonFuseable() {
        Folyam.generate(() -> 0, (s, e) -> {
            e.onNext(1);
            e.onComplete();
        })
        .test(1L, false, FusedSubscription.ANY | FusedSubscription.BOUNDARY)
        .assertFusionMode(FusedSubscription.NONE)
        .assertResult(1);
    }
}
