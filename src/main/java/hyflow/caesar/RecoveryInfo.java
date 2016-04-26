package hyflow.caesar;

import hyflow.caesar.messages.RecoveryReply;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestStatus;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by balajiarun on 3/15/16.
 */
public class RecoveryInfo {

    private final Request request;
    private final RecoveryReply[] replies;

    private final int classicQuorum;
    private int count;
    private boolean done;
    private int maxView;

    public RecoveryInfo(Request request, int numReplicas) {
        this.request = request;
        maxView = request.getView();

        replies = new RecoveryReply[numReplicas];

        count = 0;
        done = false;

        classicQuorum = ProcessDescriptor.getInstance().classicQuorum;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone() {
        done = true;
    }

    public void addReply(RecoveryReply msg, int sender) {
        replies[sender] = msg;
        count++;

        maxView = msg.getRequestView() > maxView ? msg.getRequestView() : maxView;
    }

    public RecoveryReply getReplyWithStatus(RequestStatus status) {
        return Arrays.stream(replies)
                .filter(msg -> msg.isValid() && msg.getRequestView() == maxView
                        && msg.getStatus() == status)
                .findFirst().orElse(null);
    }

    public Set<RecoveryReply> getRecoverySet() {
        Set<RecoveryReply> recoverySet = Arrays.stream(replies)
                .filter(msg -> msg.isValid() && msg.getRequestView() == maxView)
                .collect(Collectors.toSet());

        return recoverySet;
    }

    public boolean isClassicQuorum() {
        return (count >= classicQuorum);
    }

}
