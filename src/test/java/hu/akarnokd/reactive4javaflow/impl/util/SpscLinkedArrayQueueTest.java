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

import static org.junit.Assert.*;

public class SpscLinkedArrayQueueTest {

    @Test
    public void normal() {
        SpscLinkedArrayQueue<Integer> q = new SpscLinkedArrayQueue<>(4);

        assertTrue(q.isEmpty());

        for (int i = 0; i < 5; i++) {
            assertTrue(q.offer(i));
            assertFalse(q.isEmpty());
        }

        for (int i = 0; i < 5; i++) {
            assertEquals(i, q.poll().intValue());
        }

        assertTrue(q.isEmpty());
        assertNull(q.poll());
    }

    @Test
    public void clear() {
        SpscLinkedArrayQueue<Integer> q = new SpscLinkedArrayQueue<>(4);
        for (int i = 0; i < 5; i++) {
            q.offer(i);
        }

        assertFalse(q.isEmpty());

        q.clear();

        assertTrue(q.isEmpty());

        assertNull(q.poll());
    }

    @Test
    public void biOffer() {
        SpscLinkedArrayQueue<Integer> q = new SpscLinkedArrayQueue<>(8);

        assertTrue(q.isEmpty());

        for (int i = 0; i < 10; i += 2) {
            q.offer(i, i + 1);
            assertFalse(q.isEmpty());
        }

        for (int i = 0; i < 10; i++) {
            assertEquals(i, q.poll().intValue());
        }

        assertTrue(q.isEmpty());
    }

    @Test
    public void biConsume() {
        SpscLinkedArrayQueue<Integer> q = new SpscLinkedArrayQueue<>(8);

        List<Integer> ts = new ArrayList<>();

        for (int i = 0; i < 10; i += 2) {
            q.offer(i, i + 1);
            assertFalse(q.isEmpty());
        }

        for (int i = 0; i < 5; i++) {
            assertTrue(q.poll((a, b) -> {
                ts.add(a);
                ts.add(b);
            }));
        }

        assertFalse(q.poll((a, b) -> {
            ts.add(a);
            ts.add(b);
        }));

        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), ts);
    }
}
