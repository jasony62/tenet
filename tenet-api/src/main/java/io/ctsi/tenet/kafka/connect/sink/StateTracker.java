package io.ctsi.tenet.kafka.connect.sink;

import io.ctsi.tenet.kafka.connect.AbstractStatus;

import java.util.concurrent.atomic.AtomicReference;

public class StateTracker {
    private final AtomicReference<StateChange> lastState = new AtomicReference<>(new StateChange());

    /**
     * Change the current state.
     * <p>
     * This method is synchronized to ensure that all state changes are captured correctly and in the same order.
     * Synchronization is acceptable since it is assumed that state changes will be relatively infrequent.
     *
     * @param newState the current state; may not be null
     * @param now      the current time in milliseconds
     */
    public synchronized void changeState(AbstractStatus.State newState, long now) {
        // JDK8: remove synchronization by using lastState.getAndUpdate(oldState->oldState.newState(newState, now));
        lastState.set(lastState.get().newState(newState, now));
    }

    /**
     * Calculate the ratio of time spent in the specified state.
     *
     * @param ratioState the state for which the ratio is to be calculated; may not be null
     * @param now        the current time in milliseconds
     * @return the ratio of time spent in the specified state to the time spent in all states
     */
    public double durationRatio(AbstractStatus.State ratioState, long now) {
        return lastState.get().durationRatio(ratioState, now);
    }

    /**
     * Get the current state.
     *
     * @return the current state; may be null if no state change has been recorded
     */
    public AbstractStatus.State currentState() {
        return lastState.get().state;
    }

    /**
     * An immutable record of the accumulated times at the most recent state change. This class is required to
     * efficiently make {@link StateTracker} threadsafe.
     */
    private static final class StateChange {

        private final AbstractStatus.State state;
        private final long startTime;
        private final long unassignedTotalTimeMs;
        private final long runningTotalTimeMs;
        private final long pausedTotalTimeMs;
        private final long failedTotalTimeMs;
        private final long destroyedTotalTimeMs;

        /**
         * The initial StateChange instance before any state has changed.
         */
        StateChange() {
            this(null, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        StateChange(AbstractStatus.State state, long startTime, long unassignedTotalTimeMs, long runningTotalTimeMs,
                    long pausedTotalTimeMs, long failedTotalTimeMs, long destroyedTotalTimeMs) {
            this.state = state;
            this.startTime = startTime;
            this.unassignedTotalTimeMs = unassignedTotalTimeMs;
            this.runningTotalTimeMs = runningTotalTimeMs;
            this.pausedTotalTimeMs = pausedTotalTimeMs;
            this.failedTotalTimeMs = failedTotalTimeMs;
            this.destroyedTotalTimeMs = destroyedTotalTimeMs;
        }

        /**
         * Return a new StateChange that includes the accumulated times of this state plus the time spent in the
         * current state.
         *
         * @param state the new state; may not be null
         * @param now   the time at which the state transition occurs.
         * @return the new StateChange, though may be this instance of the state did not actually change; never null
         */
        public StateChange newState(AbstractStatus.State state, long now) {
            if (this.state == null) {
                return new StateChange(state, now, 0L, 0L, 0L, 0L, 0L);
            }
            if (state == this.state) {
                return this;
            }
            long unassignedTime = this.unassignedTotalTimeMs;
            long runningTime = this.runningTotalTimeMs;
            long pausedTime = this.pausedTotalTimeMs;
            long failedTime = this.failedTotalTimeMs;
            long destroyedTime = this.destroyedTotalTimeMs;
            long duration = now - startTime;
            switch (this.state) {
                case UNASSIGNED:
                    unassignedTime += duration;
                    break;
                case RUNNING:
                    runningTime += duration;
                    break;
                case PAUSED:
                    pausedTime += duration;
                    break;
                case FAILED:
                    failedTime += duration;
                    break;
                case DESTROYED:
                    destroyedTime += duration;
                    break;
            }
            return new StateChange(state, now, unassignedTime, runningTime, pausedTime, failedTime, destroyedTime);
        }

        /**
         * Calculate the ratio of time spent in the specified state.
         *
         * @param ratioState the state for which the ratio is to be calculated; may not be null
         * @param now        the current time in milliseconds
         * @return the ratio of time spent in the specified state to the time spent in all states
         */
        public double durationRatio(AbstractStatus.State ratioState, long now) {
            if (state == null) {
                return 0.0d;
            }
            long durationCurrent = now - startTime; // since last state change
            long durationDesired = ratioState == state ? durationCurrent : 0L;
            switch (ratioState) {
                case UNASSIGNED:
                    durationDesired += unassignedTotalTimeMs;
                    break;
                case RUNNING:
                    durationDesired += runningTotalTimeMs;
                    break;
                case PAUSED:
                    durationDesired += pausedTotalTimeMs;
                    break;
                case FAILED:
                    durationDesired += failedTotalTimeMs;
                    break;
                case DESTROYED:
                    durationDesired += destroyedTotalTimeMs;
                    break;
            }
            long total = durationCurrent + unassignedTotalTimeMs + runningTotalTimeMs + pausedTotalTimeMs +
                    failedTotalTimeMs + destroyedTotalTimeMs;
            return total == 0.0d ? 0.0d : (double) durationDesired / total;
        }
    }
}
