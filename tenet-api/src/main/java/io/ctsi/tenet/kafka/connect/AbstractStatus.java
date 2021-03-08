package io.ctsi.tenet.kafka.connect;

import java.util.Objects;

/**
 * kafka consumer状态处理
 * @param <T>
 */
public abstract class AbstractStatus<T> {
   private final T id;
   private final State state;
   private final String trace;
   private final String workerId;
   private final int generation;

   public AbstractStatus(T id, State state, String workerId, int generation, String trace) {
       this.id = id;
       this.state = state;
       this.workerId = workerId;
       this.generation = generation;
       this.trace = trace;
   }

   public T id() {
       return this.id;
   }

   public State state() {
       return this.state;
   }

   public String trace() {
       return this.trace;
   }

   public String workerId() {
       return this.workerId;
   }

   public int generation() {
       return this.generation;
   }

   public String toString() {
       return "Status{id=" + this.id + ", state=" + this.state + ", workerId='" + this.workerId + '\'' + ", generation=" + this.generation + '}';
   }

   public boolean equals(Object o) {
       if (this == o) {
           return true;
       } else if (o != null && this.getClass() == o.getClass()) {
           AbstractStatus<?> that = (AbstractStatus)o;
           return this.generation == that.generation && Objects.equals(this.id, that.id) && this.state == that.state && Objects.equals(this.trace, that.trace) && Objects.equals(this.workerId, that.workerId);
       } else {
           return false;
       }
   }

   public int hashCode() {
       int result = this.id != null ? this.id.hashCode() : 0;
       result = 31 * result + (this.state != null ? this.state.hashCode() : 0);
       result = 31 * result + (this.trace != null ? this.trace.hashCode() : 0);
       result = 31 * result + (this.workerId != null ? this.workerId.hashCode() : 0);
       result = 31 * result + this.generation;
       return result;
   }

   public static enum State {
       UNASSIGNED,
       RUNNING,
       PAUSED,
       FAILED,
       DESTROYED;

       private State() {
       }
   }
}
