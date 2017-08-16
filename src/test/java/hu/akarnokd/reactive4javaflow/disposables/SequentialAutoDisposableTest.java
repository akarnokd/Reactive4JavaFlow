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

package hu.akarnokd.reactive4javaflow.disposables;

import org.junit.Test;

import static org.junit.Assert.*;

public class SequentialAutoDisposableTest {

    @Test
    public void normal() {
        SequentialAutoDisposable sd = new SequentialAutoDisposable();

        BooleanAutoDisposable b1 = new BooleanAutoDisposable();

        assertTrue(sd.replace(b1));

        assertSame(b1, sd.get());

        assertSame(b1, sd.getPlain());

        assertFalse(b1.isClosed());

        BooleanAutoDisposable b2 = new BooleanAutoDisposable();

        assertTrue(sd.update(b2));

        assertTrue(b1.isClosed());

        assertFalse(b2.isClosed());

        assertFalse(sd.isClosed());

        sd.close();

        assertTrue(sd.isClosed());

        assertTrue(b2.isClosed());

        BooleanAutoDisposable b3 = new BooleanAutoDisposable();

        assertFalse(sd.replace(b3));

        assertTrue(b3.isClosed());

        BooleanAutoDisposable b4 = new BooleanAutoDisposable();

        assertFalse(sd.update(b4));

        assertTrue(b4.isClosed());
    }
}
