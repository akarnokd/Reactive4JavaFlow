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

package hu.akarnokd.reactive4javaflow.impl.util;

import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class MpscLinkedArrayQueueTest {

    @Test
    public void normal() {
        MpscLinkedArrayQueue<Integer> q = new MpscLinkedArrayQueue<>(4);

        assertTrue(q.isEmpty());

        for (int i = 0; i < 5; i++) {
            assertTrue(q.offer(i));
        }

        for (int i = 0; i < 5; i++) {
            assertEquals(i, q.poll().intValue());
        }

        assertTrue(q.isEmpty());

        for (int i = 0; i < 10; i++) {
            assertTrue(q.offer(i));
            assertFalse(q.isEmpty());
            assertEquals(i, q.poll().intValue());
            assertTrue(q.isEmpty());
            assertNull(q.poll());
        }

        assertTrue(q.isEmpty());

        for (int i = 0; i < 5; i++) {
            q.offer(i);
        }

        q.clear();

        assertTrue(q.isEmpty());
        assertNull(q.poll());

    }

    int loopCount() {
        int i = 500;

        if (System.getenv("CI") != null) {
            i = 1000;
        }

        return i;
    }

    @Test(timeout = 10000)
    public void concurrentOffer() throws Exception {
        for (int c = 1; c < 2048; c *= 2) {
            int lc = loopCount();
            for (int i = 0; i < lc; i++) {
                MpscLinkedArrayQueue<Integer> q = new MpscLinkedArrayQueue<>(c);

                AtomicInteger sync = new AtomicInteger(2);

                Thread t = new Thread(() -> {
                    if (sync.decrementAndGet() != 0) {
                        while (sync.get() != 0) ;
                    }
                    for (int j = 500; j < 1000; j++) {
                        q.offer(j);
                    }
                });

                t.start();

                if (sync.decrementAndGet() != 0) {
                    while (sync.get() != 0) ;
                }

                for (int j = 0; j < 500; j++) {
                    q.offer(j);
                }

                t.join();

                assertFalse(q.isEmpty());

                Set<Integer> set = new HashSet<>();

                for (;;) {
                    Integer v = q.poll();
                    if (v == null) {
                        break;
                    }
                    set.add(v);
                }

                assertEquals(1000, set.size());

                for (int j = 0; j < 1000; j++) {
                    assertTrue(set.contains(j));
                }

                assertTrue(q.isEmpty());
            }
        }
    }

    @Test
    public void concurrentOfferPoll() throws Exception {
        for (int c = 1; c < 2048; c *= 2) {
            int lc = loopCount();
            for (int i = 0; i < lc; i++) {
                MpscLinkedArrayQueue<Integer> q = new MpscLinkedArrayQueue<>(c);

                AtomicInteger sync = new AtomicInteger(2);

                Thread t = new Thread(() -> {
                    if (sync.decrementAndGet() != 0) {
                        while (sync.get() != 0) ;
                    }
                    for (int j = 0; j < 1000; j++) {
                        q.offer(j);
                    }
                });

                t.start();

                if (sync.decrementAndGet() != 0) {
                    while (sync.get() != 0) ;
                }

                for (int j = 0; j < 1000; j++) {
                    Integer v;
                    for (;;) {
                        v = q.poll();
                        if (v != null) {
                            break;
                        }
                    }
                    assertEquals(j, v.intValue());
                }

                t.join();

                assertTrue(q.isEmpty());
            }
        }
    }
}
