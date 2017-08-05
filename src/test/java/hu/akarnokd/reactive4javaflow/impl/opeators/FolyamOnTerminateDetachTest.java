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

package hu.akarnokd.reactive4javaflow.impl.opeators;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.hot.DirectProcessor;
import org.junit.Test;

import java.io.IOException;
import java.lang.ref.*;

import static org.junit.Assert.assertNull;

public class FolyamOnTerminateDetachTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.range(1, 5).onTerminateDetach(), 1, 2, 3, 4, 5);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().onTerminateDetach(),
                1, 2, 3, 4, 5);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .onTerminateDetach()
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .onTerminateDetach()
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void verifyDetach1() throws Exception {
        Object[] o = new Object[16];
        WeakReference<Object[]> ph = new WeakReference<>(o);

        TestConsumer<Object[]> tc = Folyam.just(o)
                .onTerminateDetach()
                .test();

        tc.assertValueAt(0, o)
        .assertNoErrors()
        .assertComplete();

        tc.clear();
        o = null;

        System.gc();
        Thread.sleep(100);

        assertNull(ph.get());
    }

    @Test
    public void verifyDetach4() throws Exception {
        Object[] o = new Object[16];
        WeakReference<Object[]> ph = new WeakReference<>(o);

        TestConsumer<Object[]> tc = Folyam.just(o)
                .onTerminateDetach()
                .test(1, false, FusedSubscription.SYNC);

        tc.assertValueAt(0, o)
                .assertNoErrors()
                .assertComplete();

        tc.clear();
        o = null;

        System.gc();
        Thread.sleep(100);

        assertNull(ph.get());
    }

    @Test
    public void verifyDetach2() throws Exception {
        Object[] o = new Object[16];
        WeakReference<Object[]> ph = new WeakReference<>(o);

        DirectProcessor<Object[]> dp = new DirectProcessor<>();

        TestConsumer<Object[]> tc = dp
                .onTerminateDetach()
                .test();

        dp.onNext(o);
        dp.onError(new IOException());

        tc.assertValueAt(0, o)
                .assertError(IOException.class)
                .assertNotComplete();

        tc.clear();
        o = null;

        System.gc();
        Thread.sleep(100);

        assertNull(ph.get());
    }


    @Test
    public void verifyDetach3() throws Exception {
        Object[] o = new Object[16];
        WeakReference<Object[]> ph = new WeakReference<>(o);

        TestConsumer<Object[]> tc = Folyam.just(o)
                .onTerminateDetach()
                .test(0);

        tc.cancel();

        o = null;

        System.gc();
        Thread.sleep(100);

        assertNull(ph.get());
    }


    @Test
    public void verifyDetach1Conditional() throws Exception {
        Object[] o = new Object[16];
        WeakReference<Object[]> ph = new WeakReference<>(o);

        TestConsumer<Object[]> tc = Folyam.just(o)
                .onTerminateDetach()
                .filter(v -> true)
                .test();

        tc.assertValueAt(0, o)
                .assertNoErrors()
                .assertComplete();

        tc.clear();
        o = null;

        System.gc();
        Thread.sleep(100);

        assertNull(ph.get());
    }


    @Test
    public void verifyDetach2Conditional() throws Exception {
        Object[] o = new Object[16];
        WeakReference<Object[]> ph = new WeakReference<>(o);

        DirectProcessor<Object[]> dp = new DirectProcessor<>();

        TestConsumer<Object[]> tc = dp
                .onTerminateDetach()
                .filter(v -> true)
                .test();

        dp.onNext(o);
        dp.onError(new IOException());

        tc.assertValueAt(0, o)
                .assertError(IOException.class)
                .assertNotComplete();

        tc.clear();
        o = null;

        System.gc();
        Thread.sleep(100);

        assertNull(ph.get());
    }


    @Test
    public void verifyDetach3Conditional() throws Exception {
        Object[] o = new Object[16];
        WeakReference<Object[]> ph = new WeakReference<>(o);

        TestConsumer<Object[]> tc = Folyam.just(o)
                .onTerminateDetach()
                .filter(v -> true)
                .test(0);

        tc.cancel();

        o = null;

        System.gc();
        Thread.sleep(100);

        assertNull(ph.get());
    }
    @Test
    public void verifyDetach4Conditional() throws Exception {
        Object[] o = new Object[16];
        WeakReference<Object[]> ph = new WeakReference<>(o);

        TestConsumer<Object[]> tc = Folyam.just(o)
                .onTerminateDetach()
                .filter(v -> true)
                .test(1, false, FusedSubscription.SYNC);

        tc.assertValueAt(0, o)
                .assertNoErrors()
                .assertComplete();

        tc.clear();
        o = null;

        System.gc();
        Thread.sleep(100);

        assertNull(ph.get());
    }
}
