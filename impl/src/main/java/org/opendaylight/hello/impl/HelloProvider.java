/*
 * Copyright © 2015 Rashmi and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.hello.impl;

import java.util.concurrent.Future;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.ReadFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ProviderContext;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.RpcRegistration;
import org.opendaylight.controller.sal.binding.api.BindingAwareProvider;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.hello.rev150105.GreetingRegistry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.hello.rev150105.GreetingRegistryBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.hello.rev150105.HelloService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.hello.rev150105.HelloWorldInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.hello.rev150105.HelloWorldOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.hello.rev150105.HelloWorldOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.hello.rev150105.greeting.registry.GreetingRegistryEntry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.hello.rev150105.greeting.registry.GreetingRegistryEntryBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.hello.rev150105.greeting.registry.GreetingRegistryEntryKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public class HelloProvider implements BindingAwareProvider, AutoCloseable, HelloService {

    private static final Logger LOG = LoggerFactory.getLogger(HelloProvider.class);
    private RpcRegistration<HelloService> helloService;
    private DataBroker db;

    @Override
    public void onSessionInitiated(ProviderContext session) {
        LOG.info("HelloProvider Session Initiated");
        db = session.getSALService(DataBroker.class);
        helloService = session.addRpcImplementation(HelloService.class, this);

        initializeDataTree(db);
    }

    @Override
    public void close() throws Exception {
        LOG.info("HelloProvider Closed");
        if (helloService != null) {
            helloService.close();
        }
    }

    @Override
    public Future<RpcResult<HelloWorldOutput>> helloWorld(HelloWorldInput input) {
        HelloWorldOutput output = new HelloWorldOutputBuilder()
                .setGreeting(readFromGreetingRegistry(input))
                .build();
        writeToGreetingRegistry(input, output);
        return RpcResultBuilder.success(output).buildFuture();
    }

    private void initializeDataTree(DataBroker db) {
        LOG.info("Preparing to initialize the greeting registry");
        WriteTransaction transaction = db.newWriteOnlyTransaction();
        InstanceIdentifier<GreetingRegistry> iid = InstanceIdentifier.create(GreetingRegistry.class);
        GreetingRegistry greetingRegistry = new GreetingRegistryBuilder()
                .build();
        transaction.put(LogicalDatastoreType.OPERATIONAL, iid, greetingRegistry);
        CheckedFuture<Void, TransactionCommitFailedException> future = transaction.submit();
        Futures.addCallback(future, new LoggingFuturesCallBack<>("Failed to create greeting registry", LOG));
    }

    private void writeToGreetingRegistry(HelloWorldInput input, HelloWorldOutput output) {
        WriteTransaction transaction = db.newWriteOnlyTransaction();
        InstanceIdentifier<GreetingRegistryEntry> iid = toInstanceIdentifier(input);
        GreetingRegistryEntry greeting = new GreetingRegistryEntryBuilder()
                .setGreeting(output.getGreeting())
                .setName(input.getName())
                .build();
        transaction.put(LogicalDatastoreType.OPERATIONAL, iid, greeting);
        CheckedFuture<Void, TransactionCommitFailedException> future = transaction.submit();
        Futures.addCallback(future, new LoggingFuturesCallBack<Void>("Failed to write greeting to greeting registry", LOG));
    }

    private InstanceIdentifier<GreetingRegistryEntry> toInstanceIdentifier(HelloWorldInput input) {
        InstanceIdentifier<GreetingRegistryEntry> iid = InstanceIdentifier.create(GreetingRegistry.class)
            .child(GreetingRegistryEntry.class, new GreetingRegistryEntryKey(input.getName()));
        return iid;
    }

    private String readFromGreetingRegistry(HelloWorldInput input) {
        String result = "Hello " + input.getName();
        ReadOnlyTransaction transaction = db.newReadOnlyTransaction();
        InstanceIdentifier<GreetingRegistryEntry> iid = toInstanceIdentifier(input);
        CheckedFuture<Optional<GreetingRegistryEntry>, ReadFailedException> future =
                transaction.read(LogicalDatastoreType.CONFIGURATION, iid);
        Optional<GreetingRegistryEntry> optional = Optional.absent();
        try {
            optional = future.checkedGet();
        } catch (ReadFailedException e) {
            LOG.warn("Reading greeting failed:",e);
        }
        if(optional.isPresent()) {
            result = optional.get().getGreeting();
        }
        return result;
    }
}
