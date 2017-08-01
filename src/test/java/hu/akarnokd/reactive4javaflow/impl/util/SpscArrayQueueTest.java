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

import static org.junit.Assert.*;

public class SpscArrayQueueTest {

    @Test
    public void normal() {
        SpscArrayQueue<Integer> q = new SpscArrayQueue<>(8);

        assertTrue(q.isEmpty());

        for (int i = 0; i < 8; i++) {
            assertTrue(q.offer(i));
            assertFalse(q.isEmpty());
        }

        assertFalse(q.offer(9));

        for (int i = 0; i < 8; i++) {
            assertEquals(i, q.poll().intValue());
        }

        assertNull(q.poll());
        assertTrue(q.isEmpty());

        assertTrue(q.offer(10));

        assertFalse(q.isEmpty());

        q.clear();

        assertTrue(q.isEmpty());
        assertNull(q.poll());
    }
}
