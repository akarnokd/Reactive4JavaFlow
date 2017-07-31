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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class BlockingFirstConsumerTest {
    @Test
    public void normal() {
        Assert.assertEquals(1, Folyam.just(1)
                .blockingFirst().get().intValue());
    }

    @Test
    public void normal2() {
        assertEquals(1, Folyam.just(1)
                .blockingFirst(1, TimeUnit.MINUTES).get().intValue());
    }

    @Test
    public void normal3() {
        assertEquals(1, Folyam.range(1, 5)
                .blockingFirst().get().intValue());
    }

    @Test
    public void empty() {
        assertFalse(Folyam.empty().blockingFirst().isPresent());
    }

    @Test(expected = IllegalArgumentException.class)
    public void blockingFirstError() {
        Folyam.error(new IllegalArgumentException()).blockingFirst();
    }


    @Test(expected = InternalError.class)
    public void blockingFirstError2() {
        Folyam.error(new InternalError()).blockingFirst();
    }

    @Test
    public void blockingFirstError3() {
        try {
            Folyam.error(new IOException()).blockingFirst();
        } catch (RuntimeException ex) {
            if (!(ex.getCause() instanceof IOException)) {
                throw new AssertionError("Wrong exception", ex);
            }
        }
    }


    @Test(expected = IllegalArgumentException.class)
    public void blockingFirstErrorTimeout() {
        Folyam.error(new IllegalArgumentException()).blockingFirst(1, TimeUnit.MINUTES);
    }


    @Test(expected = InternalError.class)
    public void blockingFirstErrorTimeout2() {
        Folyam.error(new InternalError()).blockingFirst(1, TimeUnit.MINUTES);
    }

    @Test
    public void blockingFirstErrorTimeout3() {
        try {
            Folyam.error(new IOException()).blockingFirst(1, TimeUnit.MINUTES);
        } catch (RuntimeException ex) {
            if (!(ex.getCause() instanceof IOException)) {
                throw new AssertionError("Wrong exception", ex);
            }
        }
    }

    @Test
    public void timeout() {
        try {
            Folyam.never().blockingFirst(1, TimeUnit.MILLISECONDS);
            fail("Should have thrown");
        } catch (RuntimeException ex) {
            if (!(ex.getCause() instanceof TimeoutException)) {
                throw new AssertionError("Wrong exception", ex);
            }
        }
    }

    @Test
    public void interrupt() {
        Thread.currentThread().interrupt();
        try {
            Folyam.never().blockingFirst(5, TimeUnit.SECONDS);
            fail("Should have thrown");
        } catch (RuntimeException ex) {
            if (!(ex.getCause() instanceof InterruptedException)) {
                throw new AssertionError("Wrong exception", ex);
            }
        }
    }
    @Test(timeout = 5000)
    public void interrupt2() {
        Thread.currentThread().interrupt();
        try {
            Folyam.never().blockingFirst();
            fail("Should have thrown");
        } catch (RuntimeException ex) {
            if (!(ex.getCause() instanceof InterruptedException)) {
                throw new AssertionError("Wrong exception", ex);
            }
        }
    }

    @Test
    public void defaultValue() {
        assertEquals(1, Folyam.<Integer>empty().blockingFirst(1).intValue());
    }
}
