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

package hu.akarnokd.reactive4javaflow.impl.consumers;

import hu.akarnokd.reactive4javaflow.Folyam;
import org.junit.Assert;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class BlockingSingleConsumerTest {
    @Test
    public void normal() {
        Assert.assertEquals(1, Folyam.just(1)
                .blockingSingle().intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void normal3() {
        Folyam.range(1, 5)
                .blockingSingle();
    }

    @Test(expected = NoSuchElementException.class)
    public void empty() {
        Folyam.empty().blockingSingle();
    }

    @Test(expected = IllegalArgumentException.class)
    public void error() {
        Folyam.error(new IllegalArgumentException()).blockingSingle();
    }

    @Test
    public void interrupt() {
        Thread.currentThread().interrupt();
        try {
            Folyam.never().blockingSingle();
            fail("Should have thrown");
        } catch (RuntimeException ex) {
            if (!(ex.getCause() instanceof InterruptedException)) {
                throw new AssertionError("Wrong exception", ex);
            }
        }
    }

    @Test
    public void defaultValue() {
        assertEquals(1, Folyam.<Integer>empty().blockingSingle(1).intValue());
    }
}
