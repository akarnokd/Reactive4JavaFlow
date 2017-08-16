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

package hu.akarnokd.reactive4javaflow.errors;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompositeThrowableTest {

    @Test
    public void combine() {
        Throwable t1 = CompositeThrowable.combine(null, new IOException("First"));
        Throwable t2 = CompositeThrowable.combine(t1, new IOException("Second"));
        Throwable t3 = CompositeThrowable.combine(t2, new IOException("Third"));

        assertTrue(t3.toString(), t3 instanceof CompositeThrowable);
        assertEquals(3, t3.getSuppressed().length);
    }
}
