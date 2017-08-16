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

package hu.akarnokd.reactive4javaflow.impl;

import hu.akarnokd.reactive4javaflow.TestHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueueHelperTest {

    @Test
    public void utilityClass() {
        TestHelper.checkUtilityClass(QueueHelper.class);
    }

    @Test
    public void pow() {
        assertEquals(1, QueueHelper.pow2(1));
        assertEquals(2, QueueHelper.pow2(2));
        assertEquals(4, QueueHelper.pow2(3));
        assertEquals(4, QueueHelper.pow2(4));
        assertEquals(8, QueueHelper.pow2(5));
        assertEquals(8, QueueHelper.pow2(6));
        assertEquals(8, QueueHelper.pow2(7));
        assertEquals(8, QueueHelper.pow2(8));
    }
}
