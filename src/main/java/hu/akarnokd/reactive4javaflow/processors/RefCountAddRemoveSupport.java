package hu.akarnokd.reactive4javaflow.processors;

import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;

import java.util.concurrent.Flow;

interface RefCountAddRemoveSupport<T> {

    boolean add(RefCountSubscriber<T> ps);

    void remove(RefCountSubscriber<T> ps);

    final class RefCountSubscriber<T> implements FolyamSubscriber<T>, FusedSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        final RefCountAddRemoveSupport<T> parent;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        RefCountSubscriber(FolyamSubscriber<? super T> actual, RefCountAddRemoveSupport<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
            parent.remove(this);
        }

        @Override
        public int requestFusion(int mode) {
            FusedSubscription<T> fs = this.qs;
            return fs != null ? fs.requestFusion(mode) : NONE;
        }

        @Override
        public T poll() throws Throwable {
            return qs.poll();
        }

        @Override
        public boolean isEmpty() {
            return qs.isEmpty();
        }

        @Override
        public void clear() {
            qs.clear();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                qs = (FusedSubscription<T>)subscription;
            }
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
            parent.remove(this);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
            parent.remove(this);
        }
    }

}
