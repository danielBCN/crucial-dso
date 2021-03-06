package org.infinispan.crucial.server;

import com.fasterxml.uuid.impl.RandomBasedGenerator;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.crucial.Factory;
import org.infinispan.crucial.object.Call;
import org.infinispan.crucial.object.CallConstruct;
import org.infinispan.crucial.object.CallInvoke;
import org.infinispan.crucial.object.CallResponse;
import org.infinispan.crucial.object.Reference;
import org.infinispan.crucial.utils.Context;
import org.infinispan.crucial.utils.ContextManager;
import org.infinispan.crucial.utils.Reflection;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.infinispan.crucial.server.Marshalling.marshall;
import static org.infinispan.crucial.server.Marshalling.unmarshall;
import static org.infinispan.crucial.utils.Reflection.callObject;
import static org.infinispan.crucial.utils.Reflection.hasReadOnlyMethods;

public class StateMachineInterceptor extends NonTxDistributionInterceptor{

    private static final Log log = LogFactory.getLog(StateMachineInterceptor.class);

    private ConcurrentMap<UUID, Map<Reference, CallResponse>> lastCall = new ConcurrentHashMap<>(); // key == callerID
    private Factory factory;

    @Override
    public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable{

        if (! (command.getValue() instanceof Call)) {
            return handleDefault(ctx, command);
        }

        assert (command.getKey() instanceof Reference) & (command.getValue() instanceof Call);

        Call call = (Call) command.getValue();

        Reference reference = call.getReference();
        if (log.isTraceEnabled())
            log.trace(" Accessing " + reference);

        CallResponse future;

        // FIXME elasticity

        CacheEntry<Reference, Object> entry = ctx.lookupEntry(reference);
        assert (call instanceof CallConstruct) | (entry.getValue() != null);

        if (log.isTraceEnabled()) {
            log.trace(" Received [" + call.toString() + "]");
            log.trace(" With ID " + call.getCallID() + "]");
            log.trace(" By " + call.getCallerID() + "]");
        }


        future = new CallResponse(reference, call);

        if (log.isTraceEnabled()) {
            log.trace(" lastCall=" + lastCall);
        }

        if (lastCall.containsKey(call.getCallerID())
                && lastCall.get(call.getCallerID()).containsKey(reference)
                && lastCall.get(call.getCallerID()).get(reference).getCallID().equals(call.getCallID())) {

            // call already completed
            future = lastCall.get(call.getCallerID()).get(reference);

        } else {

            try {

                ContextManager.set(
                        new Context(
                                call.getCallerID(),
                                new RandomBasedGenerator(
                                        new Random(call.getCallID().getLeastSignificantBits()
                                                + call.getCallID().getMostSignificantBits())),
                                factory));

                if (call instanceof CallInvoke) {

                    CallInvoke invocation = (CallInvoke) call;

                    assert (entry.getValue() != null);

                    Object[] args = invocation.arguments;

                    Object response;

                    try {

                        if (log.isTraceEnabled())
                            log.trace(dm.getCacheTopology().getLocalAddress() + "#" + call);

                        synchronized (entry.getValue()) { // synchronization contract
                            response = callObject(entry.getValue(), invocation.method, args);
                        }

                        future.set(response);

                    } catch (Throwable e) {
                        future.set(e);
                    }


                } else if (call instanceof CallConstruct) {

                    CallConstruct callConstruct = (CallConstruct) call;

                    if (entry.getValue() == null | callConstruct.getForceNew()) {

                        if (log.isTraceEnabled())
                            log.trace(" New [" + reference + "]");

                        entry.setValue(
                                Reflection.open(reference, callConstruct.getInitArgs()));

                    }

                    future.set(null);

                }

            } catch (Exception e) {
                throw e;
            }

        } // end compute return value

        assert ! future.isCancelled();
        assert future.isDone();

        // save return value
        if (! lastCall.containsKey(call.getCallerID())) {
            lastCall.putIfAbsent(call.getCallerID(), new HashMap<>());
        }
        lastCall.get(call.getCallerID()).put(reference, future);

        // save state if required
        if (hasReadOnlyMethods(reference.getClazz())) { // FIXME state = byte array
            synchronized (entry.getValue()) { // synchronization contract
                future.setState(unmarshall(marshall(entry.getValue())));
            }
        }

        PutKeyValueCommand clone = cf.buildPutKeyValueCommand(
                command.getKey(), entry.getValue(),
                command.getMetadata(), command.getFlagsBitSet());
        invokeNext(ctx, clone);

        if (log.isTraceEnabled())
            log.trace(" returning " + future.toString());

        return future;
    }

    public void setup(Factory factory){
        this.factory = factory;
    }

    // utils

    @Override
    protected Log getLog(){
        return log;
    }
}
