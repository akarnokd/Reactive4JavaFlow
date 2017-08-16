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

import java.util.List;

import static org.junit.Assert.*;

public class CompositeAutoDisposableTest {

    @Test
    public void normal() {
        CompositeAutoDisposable cd = new CompositeAutoDisposable();

        assertFalse(cd.isClosed());

        cd.clear();

        assertFalse(cd.isClosed());

        BooleanAutoDisposable b = new BooleanAutoDisposable();
        assertTrue(cd.add(b));

        assertEquals(1, cd.size());

        assertTrue(cd.delete(b));

        assertEquals(0, cd.size());

        assertFalse(b.isClosed());

        cd.add(b);

        assertEquals(1, cd.size());

        assertTrue(cd.remove(b));

        assertEquals(0, cd.size());

        assertTrue(b.isClosed());

        b = new BooleanAutoDisposable();

        cd.add(b);

        cd.clear();

        assertEquals(0, cd.size());

        assertTrue(b.isClosed());

        assertFalse(cd.isClosed());

        b = new BooleanAutoDisposable();

        cd.add(b);

        cd.close();

        assertTrue(b.isClosed());

        assertTrue(cd.isClosed());

        assertEquals(0, cd.size());
    }

    @Test
    public void normal2() {
        CompositeAutoDisposable cd = new CompositeAutoDisposable();

        BooleanAutoDisposable b = new BooleanAutoDisposable();

        assertFalse(cd.delete(b));

        assertFalse(cd.remove(b));

        cd.close();

        assertFalse(cd.add(b));

        assertTrue(b.isClosed());

        assertFalse(cd.delete(b));

        assertFalse(cd.remove(b));
    }

    @Test
    public void normal3() {
        BooleanAutoDisposable b1 = new BooleanAutoDisposable();
        BooleanAutoDisposable b2 = new BooleanAutoDisposable();

        CompositeAutoDisposable cd = new CompositeAutoDisposable(b1, b2);

        assertEquals(2, cd.size());

        cd.close();

        assertTrue(b1.isClosed());
        assertTrue(b2.isClosed());
    }

    @Test
    public void normal4() {
        BooleanAutoDisposable b1 = new BooleanAutoDisposable();
        BooleanAutoDisposable b2 = new BooleanAutoDisposable();

        CompositeAutoDisposable cd = new CompositeAutoDisposable(List.of(b1, b2));

        assertEquals(2, cd.size());

        cd.close();

        assertTrue(b1.isClosed());
        assertTrue(b2.isClosed());
    }
}
