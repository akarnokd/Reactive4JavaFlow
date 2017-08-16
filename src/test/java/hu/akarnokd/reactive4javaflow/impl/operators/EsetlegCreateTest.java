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
import hu.akarnokd.reactive4javaflow.disposables.BooleanAutoDisposable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class EsetlegCreateTest {

    @Test
    public void normal() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        Long[] req = { null, null };

        Esetleg.<Integer>create(e -> {
            req[0] = e.requested();
            e.onNext(1);
            req[1] = e.requested();
        })
        .safeSubscribe(tc);

        tc.assertResult(1);

        assertEquals(1, req[0].longValue());
        assertEquals(0, req[1].longValue());
    }

    @Test
    public void normal1() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        Esetleg.<Integer>create(e -> {
            e.onNext(1);
            e.onNext(2);
        })
                .safeSubscribe(tc);

        tc.assertResult(1);
    }

    @Test
    public void error() {
        TestHelper.withErrorTracking(errors -> {
            TestConsumer<Integer> tc = new TestConsumer<>();

            Boolean[] b = { null };

            Esetleg.<Integer>create(e -> {
                e.onError(new IOException());
                b[0] = e.tryOnError(new IllegalArgumentException());
                e.onError(new NullPointerException());
            })
            .safeSubscribe(tc);

            tc.assertFailure(IOException.class);

            assertFalse(b[0]);

            TestHelper.assertError(errors, 0, NullPointerException.class);
        });
    }

    @Test
    public void empty() {
        Esetleg.<Integer>create(e -> {
            e.onComplete();
            e.onComplete();
            e.onNext(1);
        })
        .test()
        .assertResult();
    }

    @Test
    public void emitterCrash() {
        Esetleg.<Integer>create(e -> {
            throw new IOException();
        })
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void resource() {
        BooleanAutoDisposable d1 = new BooleanAutoDisposable();
        BooleanAutoDisposable d2 = new BooleanAutoDisposable();
        FolyamEmitter[] b = { null, null };
        Boolean[] c = { null };

        Esetleg.<Integer>create(e -> {
            b[0] = e;
            c[0] = e.isCancelled();
            e = e.serialized();
            e.setResource(d1);
            e.setResource(d2);
        })
        .test()
        .cancel()
        .assertEmpty();

        assertTrue(d1.isClosed());
        assertTrue(d2.isClosed());
        assertTrue(b[0].isCancelled());
        assertFalse(c[0]);
    }

    @Test
    public void resourceNext() {
         BooleanAutoDisposable d1 = new BooleanAutoDisposable();
         Esetleg.<Integer>create(e -> {
             e.setResource(d1);
             e.onNext(1);
         })
         .test()
         .assertResult(1);
         assertTrue(d1.isClosed());
    }

    @Test
    public void resourceError() {
        BooleanAutoDisposable d1 = new BooleanAutoDisposable();
        Esetleg.<Integer>create(e -> {
            e.setResource(d1);
            e.onError(new IOException());
        })
            .test()
            .assertFailure(IOException.class);
        assertTrue(d1.isClosed());
    }

    @Test
    public void resourceComplete() {
        BooleanAutoDisposable d1 = new BooleanAutoDisposable();
        Esetleg.<Integer>create(e -> {
            e.setResource(d1);
            e.onComplete();
        })
                .test()
                .assertResult();
        assertTrue(d1.isClosed());
    }

    @Test
    public void nullItem() {
        Esetleg.<Integer>create(e -> {
            e.onNext(null);
        })
        .test()
        .assertFailureAndMessage(NullPointerException.class, "item == null");
    }

    @Test
    public void nullError() {
        Esetleg.<Integer>create(e -> {
            e.onError(null);
        })
        .test()
        .assertFailureAndMessage(NullPointerException.class, "throwable == null");
    }

    @Test
    public void setResourceOldCrash() {
        TestHelper.withErrorTracking(errors -> {
            Esetleg.create(e -> {
                e.setResource(() -> { throw new IOException("first"); });
                e.setResource(() -> { throw new IOException("second"); });
                e.setResource(() -> { throw new IOException("third"); });
                e.onNext(1);
            })
            .test()
            .assertResult(1);

            assertEquals(3, errors.size());

            TestHelper.assertError(errors, 0, IOException.class, "first");
            TestHelper.assertError(errors, 1, IOException.class, "second");
            TestHelper.assertError(errors, 2, IOException.class, "third");
        });
    }

    @Test
    public void immediateCancel() {
        BooleanAutoDisposable b = new BooleanAutoDisposable();
        Esetleg.create(e -> {
            e.setResource(b);
        })
        .test(0, true, 0)
        .assertEmpty();

        assertTrue(b.isClosed());
    }
}
