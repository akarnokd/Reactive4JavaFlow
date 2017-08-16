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

package hu.akarnokd.reactive4javaflow.processors;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.processors.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.*;

public class SerializedFolyamProcessorTest {

    @Test
    public void statePeeking() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        FolyamProcessor<Integer> fp = dp.toSerialized();

        assertFalse(fp.hasSubscribers());
        assertFalse(fp.hasComplete());
        assertFalse(fp.hasThrowable());
        assertNull(fp.getThrowable());

        TestConsumer<Integer> tc = fp.test();

        assertTrue(fp.hasSubscribers());
        assertFalse(fp.hasComplete());
        assertFalse(fp.hasThrowable());
        assertNull(fp.getThrowable());

        fp.onComplete();

        tc.assertResult();

        assertFalse(fp.hasSubscribers());
        assertTrue(fp.hasComplete());
        assertFalse(fp.hasThrowable());
        assertNull(fp.getThrowable());
    }

    @Test
    public void statePeeking2() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        FolyamProcessor<Integer> fp = dp.toSerialized();

        assertFalse(fp.hasSubscribers());
        assertFalse(fp.hasComplete());
        assertFalse(fp.hasThrowable());
        assertNull(fp.getThrowable());

        TestConsumer<Integer> tc = fp.test();

        assertTrue(fp.hasSubscribers());
        assertFalse(fp.hasComplete());
        assertFalse(fp.hasThrowable());
        assertNull(fp.getThrowable());

        fp.onError(new IOException());

        tc.assertFailure(IOException.class);

        assertFalse(fp.hasSubscribers());
        assertFalse(fp.hasComplete());
        assertTrue(fp.hasThrowable());
        assertTrue(fp.getThrowable() + "", fp.getThrowable() instanceof IOException);
    }

    @Test(expected = NullPointerException.class)
    public void onNextNull() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        FolyamProcessor<Integer> fp = dp.toSerialized();

        fp.onNext(null);
    }

    @Test(expected = NullPointerException.class)
    public void onErrorNull() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        FolyamProcessor<Integer> fp = dp.toSerialized();

        fp.onError(null);
    }

    @Test
    public void onSubscribeRequestCancel() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        SerializedFolyamProcessor<Integer> fp = (SerializedFolyamProcessor<Integer>)dp1.toSerialized();

        dp.subscribe(fp);

        assertTrue(dp.hasSubscribers());

        fp.cancel();

        assertFalse(dp.hasSubscribers());
    }

    @Test
    public void onNextOnNextRace() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Integer> dp = new DirectProcessor<>();
            FolyamProcessor<Integer> fp = dp.toSerialized();

            TestConsumer<Integer> tc = fp.test();

            Runnable r1 = () -> fp.onNext(1);
            Runnable r2 = () -> fp.onNext(2);

            TestHelper.race(r1, r2);

            tc.assertValueSet(Set.of(1, 2));
        }
    }

    @Test
    public void onNextOnErrorRace() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Integer> dp = new DirectProcessor<>();
            FolyamProcessor<Integer> fp = dp.toSerialized();

            TestConsumer<Integer> tc = fp.test();

            Throwable ex = new IOException();

            Runnable r1 = () -> fp.onNext(1);
            Runnable r2 = () -> fp.onError(ex);

            TestHelper.race(r1, r2);

            int c = tc.values().size();
            if (c >= 1) {
                tc.assertFailure(IOException.class, 1);
            } else {
                tc.assertFailure(IOException.class);
            }
        }
    }

    @Test
    public void onNextOnCompleteRace() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Integer> dp = new DirectProcessor<>();
            FolyamProcessor<Integer> fp = dp.toSerialized();

            TestConsumer<Integer> tc = fp.test();

            Runnable r1 = () -> fp.onNext(1);
            Runnable r2 = fp::onComplete;

            TestHelper.race(r1, r2);

            int c = tc.values().size();
            if (c >= 1) {
                tc.assertResult(1);
            } else {
                tc.assertResult();
            }
        }
    }
}
