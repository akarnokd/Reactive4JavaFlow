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

package hu.akarnokd.reactive4javaflow;

import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.util.OpenHashSet;

import java.util.Objects;

public final class CompositeAutoDisposable implements AutoDisposable {

    volatile boolean disposed;

    OpenHashSet<AutoDisposable> set;

    public CompositeAutoDisposable() {

    }

    public CompositeAutoDisposable(AutoDisposable... disposables) {
        int c = disposables.length;
        if (c != 0) {
            set = new OpenHashSet<>(c);
            for (AutoDisposable d : disposables) {
                set.add(d);
            }
        }
    }

    public CompositeAutoDisposable(Iterable<? extends AutoDisposable> disposables) {
        for (AutoDisposable d : disposables) {
            if (set == null) {
                set = new OpenHashSet<>();
            }
            set.add(d);
        }
    }

    @Override
    public void close() {
        if (!disposed) {
            OpenHashSet<AutoDisposable> set;
            synchronized (this) {
                if (disposed) {
                    return;
                }
                set = this.set;
                this.set = null;
                disposed = true;
            }
            Object[] entries = set.keys();
            if (entries != null) {
                for (Object e : entries) {
                    if (e != null) {
                        ((AutoDisposable) e).close();
                    }                }
            }
        }
    }

    public boolean add(AutoDisposable d) {
        Objects.requireNonNull(d, "d == null");
        if (!disposed) {
            synchronized (this) {
                if (!disposed) {
                    OpenHashSet<AutoDisposable> set = this.set;
                    if (set == null) {
                        set = new OpenHashSet<>();
                        this.set = set;
                    }
                    set.add(d);
                    return true;
                }
            }
        }
        return false;
    }

    public boolean remove(AutoDisposable d) {
        Objects.requireNonNull(d, "d == null");
        if (!disposed) {
            synchronized (this) {
                if (!disposed) {
                    OpenHashSet<AutoDisposable> set = this.set;
                    if (set == null || !set.remove(d)) {
                        return false;
                    }
                }
            }
            d.close();
            return true;
        }
        return false;
    }

    public boolean delete(AutoDisposable d) {
        Objects.requireNonNull(d, "d == null");
        if (!disposed) {
            synchronized (this) {
                if (!disposed) {
                    OpenHashSet<AutoDisposable> set = this.set;
                    return set != null && set.remove(d);
                }
            }
        }
        return false;
    }

    public void clear() {
        if (!disposed) {
            OpenHashSet<AutoDisposable> set;
            synchronized (this) {
                if (disposed) {
                    return;
                }
                set = this.set;
                this.set = null;
            }
            Object[] entries = set.keys();
            if (entries != null) {
                for (Object o : entries) {
                    ((AutoDisposable)o).close();
                }
            }
        }
    }

    public int size() {
        synchronized (this) {
            return set != null ? set.size() : 0;
        }
    }
}
