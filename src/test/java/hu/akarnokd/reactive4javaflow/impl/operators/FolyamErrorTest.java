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

package hu.akarnokd.reactive4javaflow.impl.operators;

import hu.akarnokd.reactive4javaflow.Folyam;
import hu.akarnokd.reactive4javaflow.fused.FusedDynamicSource;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FolyamErrorTest {

    @Test
    public void normal() {
        Folyam.error(new IOException())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void normalFused() {
        FusedDynamicSource<Object> f = (FusedDynamicSource<Object>)Folyam.error(new IOException());
        try {
            f.value();
            fail("Should have thrown");
        } catch (Throwable ex) {
            assertTrue(ex.toString(), ex instanceof IOException);
        }
    }

    @Test
    public void callback() {
        Folyam.error(IOException::new)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void callbackNull() {
        Folyam.error(() -> null)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void callbackFused() {
        FusedDynamicSource<Object> f = (FusedDynamicSource<Object>)Folyam.error(IOException::new);
        try {
            f.value();
            fail("Should have thrown");
        } catch (Throwable ex) {
            assertTrue(ex.toString(), ex instanceof IOException);
        }
    }
}
