package io.openenterprise.daisy;

import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

@Getter
public class InvocationContext {

    protected final UUID id;

    @Setter
    protected Invocation<?> currentInvocation;

    protected List<Invocation<?>> pastInvocations = new LinkedList<>();

    public InvocationContext(UUID id) {
        this.id = id;
    }

}



