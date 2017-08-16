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

import hu.akarnokd.reactive4javaflow.Folyam;
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class FolyamToCompletableFutureTest {

    @Test
    public void empty() throws Exception {
        assertNull(Folyam.empty()
                .toCompletableFuture()
                .get());
    }

    @Test
    public void just() throws Exception {
        assertEquals(1, Folyam.just(1).toCompletableFuture().get().intValue());
    }

    @Test
    public void range() throws Exception {
        assertEquals(5, Folyam.range(1, 5).toCompletableFuture().get().intValue());
    }

    @Test
    public void error() throws Exception {
        try {
            Folyam.error(new IOException())
                    .toCompletableFuture()
                    .get();
            fail("Should have thrown");
        } catch (ExecutionException ex) {
            assertTrue(ex.toString(), ex.getCause() instanceof IOException);
        }
    }

    @Test
    public void completedBefore() throws Exception {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        CompletableFuture<Integer> cf = dp.toCompletableFuture();

        assertTrue(dp.hasSubscribers());

        cf.complete(1);

        assertFalse(dp.hasSubscribers());

        assertEquals(1, cf.get().intValue());
    }

    @Test
    public void errorBefore() throws Exception {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        CompletableFuture<Integer> cf = dp.toCompletableFuture();

        assertTrue(dp.hasSubscribers());

        cf.completeExceptionally(new IOException());

        assertFalse(dp.hasSubscribers());

        try {
            cf.get();
            fail("Should have thrown!");
        } catch (ExecutionException ex) {
            assertTrue(ex.toString(), ex.getCause() instanceof IOException);
        }
    }

    @Test
    public void cancelBefore() throws Exception {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        CompletableFuture<Integer> cf = dp.toCompletableFuture();

        assertTrue(dp.hasSubscribers());

        cf.cancel(true);

        assertFalse(dp.hasSubscribers());

        try {
            cf.get();
            fail("Should have thrown!");
        } catch (CancellationException ex) {
            // expected
        }
    }

}
