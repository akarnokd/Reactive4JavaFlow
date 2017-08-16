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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OpenHashSetTest {

    @Test
    public void duplicates() {
        OpenHashSet<Integer> set = new OpenHashSet<>(4);
        for (int i = 0; i < 200; i++) {
            assertTrue(set.add(i));
            assertFalse(set.add(i));
        }
        for (int i = 0; i < 200; i++) {
            assertTrue(set.remove(i));
            assertFalse(set.remove(i));
        }
    }

}
