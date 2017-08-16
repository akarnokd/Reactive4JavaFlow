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

package hu.akarnokd.reactive4javaflow.impl;

/**
 * Utility methods to validate operator parameters.
 */
public final class ParameterHelper {

    /** Utility class. */
    private ParameterHelper() {
        throw new IllegalStateException("No instances!");
    }

    public static void verifyPositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + ": " + value + " <= 0");
        }
    }

    public static void verifyPositive(long value, String name) {
        if (value <= 0L) {
            throw new IllegalArgumentException(name + ": " + value + " <= 0L");
        }
    }


    public static void verifyNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + ": " + value + " < 0");
        }
    }

    public static void verifyNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + ": " + value + " < 0L");
        }
    }
}
