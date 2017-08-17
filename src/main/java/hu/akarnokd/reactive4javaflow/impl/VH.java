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

import java.lang.invoke.*;

/**
 * Utility class to turn lookup failures into errors and
 * avoiding the need for static initializer with uncoverable
 * catch clauses
 */
public final class VH {

    /** Utility class. */
    private VH() {
        throw new IllegalStateException("No instances!");
    }

    public static VarHandle find(MethodHandles.Lookup lookup, Class<?> parent, String field, Class<?> type) {
        try {
            return lookup.findVarHandle(parent, field, type);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public static VarHandle findStatic(MethodHandles.Lookup lookup, Class<?> parent, String field, Class<?> type) {
        try {
            return lookup.findStaticVarHandle(parent, field, type);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }
}
