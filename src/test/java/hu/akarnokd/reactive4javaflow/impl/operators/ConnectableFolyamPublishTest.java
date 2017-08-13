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
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.SequentialAutoDisposable;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class ConnectableFolyamPublishTest {

    @Test
    public void normal() {
        ConnectableFolyam<Integer> cf = Folyam.range(1, 5).publish();

        TestConsumer<Integer> tc1 = cf.test();
        TestConsumer<Integer> tc2 = cf.test();

        cf.connect();

        tc1.assertResult(1, 2, 3, 4, 5);
        tc2.assertResult(1, 2, 3, 4, 5);

        cf.test().assertResult();

        cf.reset();

        tc1 = cf.test();
        tc2 = cf.test();

        cf.connect();

        tc1.assertResult(1, 2, 3, 4, 5);
        tc2.assertResult(1, 2, 3, 4, 5);

        cf.test().assertResult();
    }

    @Test
    public void connect() {
        ConnectableFolyam<Integer> cf = Folyam.range(1, 5).publish(2);

        SequentialAutoDisposable sd = new SequentialAutoDisposable();

        cf.connect(sd::replace);

        assertNotNull(sd.get());

        AutoDisposable ad = cf.connect();

        assertSame(ad, sd.get());

        cf.test().assertResult(1, 2, 3, 4, 5);

        cf.test().assertResult();
    }
}
