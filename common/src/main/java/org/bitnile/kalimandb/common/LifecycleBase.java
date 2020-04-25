package org.bitnile.kalimandb.common;

import org.bitnile.kalimandb.common.exception.LifecycleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of {@link Lifecycle}, it's better to inherit this class and
 * implement the abstract method in it.
 * <p>
 * For example:
 * <pre>
 * public class OuterStore extends LifecycleBase {
 *     private List&lt;InnerStore&gt; innerStores;
 *
 *     public OuterStore(List&lt;InnerStore&gt; innerStores) {
 *         this.innerStores = innerStores;
 *     }
 *
 *     \@Override
 *     protected void initInternal() {
 *         // Do some initialization itself,
 *         // then initialize its components in turn
 *         for(int i = 0; i &lt; innerStores.size(); i++){
 *             innerStores.get(i).init();
 *         }
 *     }
 *
 *     // ...
 * }
 *
 * class InnerStore extends LifecycleBase{
 *     \@Override
 *     protected void initInternal() {
 *         // Do some initialization itself
 *     }
 *
 *     // ...
 * }
 * </pre>
 *
 * <p>
 * ATTENTION: Wrong state machine input will result in {@link LifecycleException} throwing, this may usually happen when
 * multithreading, one thread is much ahead of the other, which will cause the slower thread to make an obsolete input,
 * and then the exception will be thrown, although the methods of the state machine is thread safe.
 *
 * For the convenience, we provide the default implementation for initInternal, startInternal, stopInternal
 * and destroyInternal method (just print log).
 *
 * @author hecenjie
 */
public abstract class LifecycleBase implements Lifecycle {

    private volatile LifecycleState state = LifecycleState.NEW;

    private static final Logger logger = LoggerFactory.getLogger(LifecycleBase.class);

    /**
     * Only can the NEW state change to INITIALIZED.
     */
    @Override
    public synchronized void init() {
        if (!state.equals(LifecycleState.NEW)) {
            throw new LifecycleException("State should be NEW before initializing");
        }
        try {
            setStateInternal(LifecycleState.INITIALIZING);
            initInternal();
            setStateInternal(LifecycleState.INITIALIZED);
        } catch (Throwable t) {
            setStateInternal(LifecycleState.FAILED);
            throw new LifecycleException("Init failed", t);
        }
    }

    /**
     * If the state is STARTING or STARTED already, this method will be ineffective.
     * If the state is NEW, {@link LifecycleBase#init()} will be called automatically and then continue to start.
     * If the state is INITIALIZED or STOPPED, start it.
     * In other cases, throws a {@link LifecycleException}.
     */
    @Override
    public synchronized void start() {
        if (LifecycleState.STARTING.equals(state) || LifecycleState.STARTED.equals(state)) {
            logger.info("STARTING or STARTED already");
            return;
        }

        if (state.equals(LifecycleState.NEW)) {
            init();
        }

        if (LifecycleState.INITIALIZED.equals(state) || LifecycleState.STOPPED.equals(state)) {
            try {
                setStateInternal(LifecycleState.STARTING);
                startInternal();
                setStateInternal(LifecycleState.STARTED);
            } catch (Throwable t) {
                setStateInternal(LifecycleState.FAILED);
                throw new LifecycleException("Start failed", t);
            }
        } else {
            throw new LifecycleException("State should be INITIALIZED or STOPPED before starting");
        }
    }

    /**
     * If the state is STOPPING or STOPPED already, this method will be ineffective.
     * If the state is STARTED or FAILED, stop it.
     * If the state is INITIALIZED, change the state to STOPPED and return directly.
     * In other cases, throws a {@link LifecycleException}.
     */
    @Override
    public synchronized void stop() {
        if (LifecycleState.STOPPING.equals(state) || LifecycleState.STOPPED.equals(state)) {
            logger.info("STOPPING or STOPPED already");
            return;
        }

        if (LifecycleState.INITIALIZED.equals(state)) {
            setStateInternal(LifecycleState.STOPPED);
            return;
        }

        if (LifecycleState.STARTED.equals(state) || LifecycleState.FAILED.equals(state)) {
            try {
                setStateInternal(LifecycleState.STOPPING);
                stopInternal();
                setStateInternal(LifecycleState.STOPPED);
            } catch (Throwable t) {
                setStateInternal(LifecycleState.FAILED);
                throw new LifecycleException("Stop failed", t);
            }
        } else {
            throw new LifecycleException("State must be STARTED before stopping");
        }
    }

    /**
     * If the state is DESTROYING or DESTROYED already, this method will be ineffective.
     * If the state is NEW, change the state to DESTROYED and return directly.
     * If the state is FAILED, stop first and continue to destroy.
     * <p>
     * In other cases, throws a {@link LifecycleException}.
     */
    @Override
    public synchronized void destroy() {
        if (LifecycleState.DESTROYING.equals(state) || LifecycleState.DESTROYED.equals(state)) {
            logger.info("DESTROYING or DESTROYED already");
            return;
        }

        if (LifecycleState.NEW.equals(state)) {
            setStateInternal(LifecycleState.DESTROYED);
            return;
        }

        if (LifecycleState.FAILED.equals(state)) {
            stop();
        }

        if (LifecycleState.STOPPED.equals(state) || LifecycleState.INITIALIZED.equals(state)) {
            try {
                setStateInternal(LifecycleState.DESTROYING);
                destroyInternal();
                setStateInternal(LifecycleState.DESTROYED);
            } catch (Throwable t) {
                setStateInternal(LifecycleState.FAILED);
                throw new LifecycleException("Failed to destroy", t);
            }
        } else {
            throw new LifecycleException("State must be STOPPED or INITIALIZED before destroying");
        }
    }

    protected void initInternal() {
        // NOOP
    }

    protected void startInternal() {
        // NOOP
    }

    protected void stopInternal() {
        // NOOP
    }

    protected void destroyInternal() {
        // NOOP
    }

    private synchronized void setStateInternal(LifecycleState state) {
        this.state = state;
    }

    @Override
    public LifecycleState getState() {
        return state;
    }

    @Override
    public String getStateName() {
        return state.name();
    }
}