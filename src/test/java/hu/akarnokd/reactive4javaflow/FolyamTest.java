/*
 * Copyright 2016-2017 David Karnok
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

package hu.akarnokd.reactive4javaflow;

import org.junit.Test;

import java.lang.reflect.*;

import static org.junit.Assert.*;


public class FolyamTest {

    @Test
    public void helloWorld() {
        Folyam.just("Hello World!")
                .test()
                .assertResult("Hello World!");
    }

    void method(int paramName) {
        // deliberately empty
    }

    @Test
    public void javacParametersEnabled() throws Exception {
        assertEquals("Please enable saving parameter names via the -parameters javac argument",
                "paramName",
                getClass()
                .getDeclaredMethod("method", Integer.TYPE)
                .getParameters()[0].getName());
    }

    void checkFinalMethods(Class<?> clazz) {
        for (Method m : clazz.getMethods()) {

            if ((m.getModifiers() & (Modifier.STATIC | Modifier.ABSTRACT)) == 0
                    && m.getDeclaringClass() == clazz) {
                assertTrue(m.toString(), (m.getModifiers() & Modifier.FINAL) != 0);
            }

        }
    }

    @Test
    public void folyamFinalMethods() {
        checkFinalMethods(Folyam.class);
    }

    @Test
    public void esetlegFinalMethods() {
        checkFinalMethods(Esetleg.class);
    }

    @Test
    public void parallelFolyamFinalMethods() {
        checkFinalMethods(ParallelFolyam.class);
    }

    @Test
    public void connectableFolyamFinalMethods() {
        checkFinalMethods(ConnectableFolyam.class);
    }
}
