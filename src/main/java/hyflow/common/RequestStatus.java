package hyflow.common;

/**
 * Created by balajiarun on 3/10/16.
 */
public enum RequestStatus {
    Waiting,

    PreFastPending,
    FastPending,

    PreSlowPending,
    SlowPending,

    Rejected,

    PreAccepted,
    Accepted,

    PreStable,
    Stable,
    Delivered
}
