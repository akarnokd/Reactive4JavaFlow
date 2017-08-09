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

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import org.junit.Test;

import java.io.IOException;

public class FolyamOnBackpressureBufferAllTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .onBackpressureBuffer(),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .onBackpressureBuffer(4),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .onBackpressureBuffer()
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void errorBackpressured() {
        Folyam.error(new IOException())
                .onBackpressureBuffer()
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .onBackpressureBuffer()
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditionalBackpressured() {
        Folyam.error(new IOException())
                .onBackpressureBuffer()
                .filter(v -> true)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void errorFused() {
        Folyam.error(new IOException())
                .onBackpressureBuffer()
                .test(0, false, FusedSubscription.ANY)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorFusedConditional() {
        Folyam.error(new IOException())
                .onBackpressureBuffer()
                .filter(v -> true)
                .test(0, false, FusedSubscription.ANY)
                .assertFailure(IOException.class);
    }

    @Test
    public void normalFusionRejected() {
        Folyam.range(1, 5)
                .onBackpressureBuffer()
                .test(10, false, FusedSubscription.SYNC)
                .assertFusionMode(FusedSubscription.NONE)
                .assertResult(1, 2, 3, 4 , 5);
    }

    @Test
    public void fusedConditionalEmpty() {
        Folyam.empty()
                .onBackpressureBuffer()
                .filter(v -> true)
                .test(0, false, FusedSubscription.ANY)
                .assertResult();
    }
}
