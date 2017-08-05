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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.*;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class FolyamBlockingIterableTest {

    @Test(timeout = 1000)
    public void simple() {
        Iterable<Integer> iter = Folyam.range(0, 5)
                .blockingIterable();

        Iterator<Integer> itor = iter.iterator();
        for (int i = 0; i < 5; i++) {
            assertTrue(itor.hasNext());
            assertEquals(i, itor.next().intValue());
        }
        assertFalse(itor.hasNext());
        try {
            itor.next();
            fail("Should have thrown");
        } catch (NoSuchElementException ex) {
            // expected
        }
    }

    @Test(timeout = 1000)
    public void normal() {
        Iterable<Integer> iter = Folyam.range(0, 1000)
                .blockingIterable();

        Iterator<Integer> itor = iter.iterator();
        for (int i = 0; i < 1000; i++) {
            assertTrue(itor.hasNext());
            assertEquals(i, itor.next().intValue());
        }
        assertFalse(itor.hasNext());
    }

    @Test(timeout = 1000)
    public void normal2() {
        Iterable<Integer> iter = Folyam.range(0, 1000).hide()
                .blockingIterable();

        Iterator<Integer> itor = iter.iterator();
        for (int i = 0; i < 1000; i++) {
            assertTrue(itor.hasNext());
            assertEquals(i, itor.next().intValue());
        }
        assertFalse(itor.hasNext());
    }

    @Test(timeout = 1000)
    public void normalAsync1() {
        Iterable<Integer> iter = Folyam.range(0, 1000)
                .subscribeOn(SchedulerServices.single())
                .blockingIterable();

        Iterator<Integer> itor = iter.iterator();
        for (int i = 0; i < 1000; i++) {
            assertTrue(itor.hasNext());
            assertEquals(i, itor.next().intValue());
        }
        assertFalse(itor.hasNext());
    }

    @Test(timeout = 1000)
    public void normalAsync2() {
        Iterable<Integer> iter = Folyam.range(0, 1000)
                .observeOn(SchedulerServices.single())
                .blockingIterable();

        Iterator<Integer> itor = iter.iterator();
        for (int i = 0; i < 1000; i++) {
            assertTrue(itor.hasNext());
            assertEquals(i, itor.next().intValue());
        }
        assertFalse(itor.hasNext());
    }

    @Test(timeout = 1000)
    public void normalAsync3() {
        Iterable<Integer> iter = Folyam.range(0, 1000).hide()
                .observeOn(SchedulerServices.single())
                .blockingIterable();

        Iterator<Integer> itor = iter.iterator();
        for (int i = 0; i < 1000; i++) {
            assertTrue(itor.hasNext());
            assertEquals(i, itor.next().intValue());
        }
        assertFalse(itor.hasNext());
    }

    @Test(timeout = 1000)
    public void normalAsync4() {
        Iterable<Integer> iter = Folyam.range(0, 1000)
                .observeOn(SchedulerServices.single())
                .blockingIterable(1);

        Iterator<Integer> itor = iter.iterator();
        for (int i = 0; i < 1000; i++) {
            assertTrue(itor.hasNext());
            assertEquals(i, itor.next().intValue());
        }
        assertFalse(itor.hasNext());
    }


    @Test(timeout = 1000)
    public void normalAsync5() {
        Iterable<Integer> iter = Folyam.range(0, 1000)
                .hide()
                .observeOn(SchedulerServices.single())
                .blockingIterable(1);

        Iterator<Integer> itor = iter.iterator();
        for (int i = 0; i < 1000; i++) {
            assertTrue(itor.hasNext());
            assertEquals(i, itor.next().intValue());
        }
        assertFalse(itor.hasNext());
    }

    @Test(timeout = 1000, expected = InternalError.class)
    public void errorError() {
        Folyam.error(new InternalError())
                .blockingIterable()
                .forEach(e -> { });
    }


    @Test(timeout = 1000, expected = IllegalArgumentException.class)
    public void errorRuntimeException() {
        Folyam.error(new IllegalArgumentException())
                .blockingIterable()
                .forEach(e -> { });
    }

    @Test(timeout = 1000)
    public void errorException() {
        try {
            Folyam.error(new IOException())
                    .blockingIterable()
                    .forEach(e -> {
                    });
        } catch (RuntimeException ex) {
            assertTrue(ex.toString(), ex.getCause() instanceof IOException);
        }
    }

    @Test
    public void syncFusedPollFails() {
        try {
            new Folyam<Integer>() {
                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
                }
            }
                    .blockingIterable().forEach(v -> {
            });
        } catch (Throwable ex) {
            assertTrue(ex.toString(), ex.getCause() instanceof IOException);
        }
    }


    @Test
    public void asyncFusedPollFails() {
        try {
            new Folyam<Integer>() {
                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    s.onSubscribe(new FailingFusedSubscription(FusedSubscription.ASYNC));
                }
            }
                    .blockingIterable().forEach(v -> {
            });
        } catch (Throwable ex) {
            assertTrue(ex.toString(), ex.getCause() instanceof IOException);
        }
    }


    @Test(timeout = 1000)
    public void interrupt() {
        Thread.currentThread().interrupt();
        try {
            Folyam.<Integer>never()
                    .blockingIterable().forEach(v -> { });
        } catch (Throwable ex) {
            assertTrue(ex.toString(), ex.getCause() instanceof InterruptedException);
        } finally {
            Thread.interrupted();
        }
    }

}
