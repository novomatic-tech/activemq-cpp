/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "FailoverTransport.h"

#include <activemq/commands/ConnectionControl.h>
#include <activemq/commands/ShutdownInfo.h>
#include <activemq/commands/RemoveInfo.h>
#include <activemq/transport/TransportRegistry.h>
#include <activemq/threads/DedicatedTaskRunner.h>
#include <activemq/threads/CompositeTaskRunner.h>
#include <activemq/transport/failover/BackupTransport.h>
#include <activemq/transport/failover/FailoverTransportListener.h>
#include <activemq/transport/failover/CloseTransportsTask.h>
#include <activemq/transport/failover/URIPool.h>
#include <decaf/util/Random.h>
#include <decaf/util/StringTokenizer.h>
#include <decaf/util/LinkedList.h>
#include <decaf/util/StlMap.h>
#include <decaf/util/concurrent/TimeUnit.h>
#include <decaf/util/concurrent/Mutex.h>
#include <decaf/lang/System.h>
#include <decaf/lang/Integer.h>

using namespace std;
using namespace activemq;
using namespace activemq::state;
using namespace activemq::commands;
using namespace activemq::exceptions;
using namespace activemq::threads;
using namespace activemq::transport;
using namespace activemq::transport::failover;
using namespace decaf;
using namespace decaf::io;
using namespace decaf::net;
using namespace decaf::util;
using namespace decaf::util::concurrent;
using namespace decaf::lang;
using namespace decaf::lang::exceptions;

////////////////////////////////////////////////////////////////////////////////
namespace activemq {
namespace transport {
namespace failover {

    class FailoverTransportImpl {
    private:

        FailoverTransportImpl(const FailoverTransportImpl&);
        FailoverTransportImpl& operator= (const FailoverTransportImpl&);

        static const int DEFAULT_INITIAL_RECONNECT_DELAY;
        static const int INFINITE_WAIT;

    public:

        AtomicBoolean closed;
        bool connected;
        AtomicBoolean started;
        bool randomize;

        long long timeout;
        long long initialReconnectDelay;
        long long maxReconnectDelay;
        long long backOffMultiplier;
        bool useExponentialBackOff;
        bool initialized;
        int maxReconnectAttempts;
        int startupMaxReconnectAttempts;
        int connectFailures;
        long long reconnectDelay;
        bool trackMessages;
        bool trackTransactionProducers;
        int maxCacheSize;
        int maxPullCacheSize;
        bool connectionInterruptProcessingComplete;
        bool firstConnection;
        bool updateURIsSupported;
        bool reconnectSupported;
        bool rebalanceUpdateURIs;
        bool backupsEnabled;
        bool priorityBackup;
        bool autoPriority;

        bool doRebalance;
        bool connectedToPriority;
        bool backupRemoved;

        mutable Mutex reconnectMutex;
        mutable Mutex backupMutex;
        mutable Mutex sleepMutex;
        mutable Mutex listenerMutex;

        StlMap<int, Pointer<Command> > requestMap;

        LinkedList<URI> uris;
        LinkedList<URI> updated;
        LinkedList<URI> priorityList;
        LinkedList<Pointer<BackupTransport> > backups;
        int backupPoolSize;
        bool priorityBackupAvailable;

        Pointer<URI> connectedTransportURI;
        Pointer<URI> backupTransportURI;
        Pointer<Transport> connectedTransport;
        Pointer<Transport> backupTransport;
        Pointer<Exception> connectionFailure;
        Pointer<CloseTransportsTask> closeTask;
        Pointer<CompositeTaskRunner> taskRunner;
        Pointer<TransportListener> disposedListener;
        Pointer<FailoverTransportListener> myTransportListener;

        TransportListener* transportListener;

        state::ConnectionStateTracker stateTracker;
        FailoverTransport* parent;

        std::vector<std::string> logCategories;

        FailoverTransportImpl(FailoverTransport* parent_) :
            closed(false),
            connected(false),
            started(false),
            randomize(true),
            timeout(INFINITE_WAIT),
            initialReconnectDelay(DEFAULT_INITIAL_RECONNECT_DELAY),
            maxReconnectDelay(1000*30),
            backOffMultiplier(2),
            useExponentialBackOff(true),
            initialized(false),
            maxReconnectAttempts(INFINITE_WAIT),
            startupMaxReconnectAttempts(INFINITE_WAIT),
            connectFailures(0),
            reconnectDelay(DEFAULT_INITIAL_RECONNECT_DELAY),
            trackMessages(false),
            trackTransactionProducers(true),
            maxCacheSize(128*1024),
            maxPullCacheSize(10),
            connectionInterruptProcessingComplete(false),
            firstConnection(true),
            updateURIsSupported(true),
            reconnectSupported(true),
            rebalanceUpdateURIs(true),
            backupsEnabled(false),
            priorityBackup(false),
            autoPriority(false),
            doRebalance(false),
            connectedToPriority(false),
            backupRemoved(false),
            reconnectMutex(),
            sleepMutex(),
            listenerMutex(),
            requestMap(),
            uris(),
            updated(),
            priorityList(),
            backupPoolSize(1),
            priorityBackupAvailable(false),
            connectedTransportURI(),
            backupTransportURI(),
            connectedTransport(),
            backupTransport(),
            connectionFailure(),
            closeTask(new CloseTransportsTask()),
            taskRunner(new CompositeTaskRunner()),
            disposedListener(),
            myTransportListener(new FailoverTransportListener(parent_)),
            transportListener(NULL),
            parent(parent_) {

            this->taskRunner->addTask(parent_);
            this->taskRunner->addTask(this->closeTask.get());
            this->stateTracker.setTrackTransactions(true);

            logCategories.push_back("activemq");
            logCategories.push_back("failover");
        }

        bool isPriority(const decaf::net::URI& uri) {
            if (!priorityBackup) {
                return false;
            }
            if (!priorityList.isEmpty()) {
                return priorityList.contains(uri);
            }
            return uris.indexOf(uri) == 0;
        }

        List<URI>& getConnectList() {
            // Pick an appropriate URI pool, updated is always preferred if updates are
            // enabled and we have any, otherwise we fallback to our original list so that
            // we ensure we always try something.
            List<URI>& uris = this->uris;
            if (this->updateURIsSupported && !this->updated.isEmpty()) {
                uris = this->updated;
            }
            return uris;
        }

        void doDelay() {
            if (reconnectDelay > 0) {
                synchronized (&sleepMutex) {
                    try {
                        sleepMutex.wait(reconnectDelay);
                    } catch (InterruptedException& e) {
                        Thread::currentThread()->interrupt();
                    }
                }
            }

            if (useExponentialBackOff) {
                // Exponential increment of reconnect delay.
                reconnectDelay *= backOffMultiplier;
                if (reconnectDelay > maxReconnectDelay) {
                    reconnectDelay = maxReconnectDelay;
                }
            }
        }

        int calculateReconnectAttemptLimit() const {
            int maxReconnectValue = maxReconnectAttempts;
            if (firstConnection && startupMaxReconnectAttempts != INFINITE_WAIT) {
                maxReconnectValue = startupMaxReconnectAttempts;
            }
            return maxReconnectValue;
        }

        bool canReconnect() const {
            int limit = calculateReconnectAttemptLimit();
            return started.get() && (limit == INFINITE_WAIT || connectFailures < limit);
        }

        /**
         * This must be called with the reconnect mutex locked.
         */
        void propagateFailureToExceptionListener() {
            synchronized(&listenerMutex) {
                if (this->transportListener != NULL) {

                    Pointer<IOException> ioException;
                    try {
                        ioException = this->connectionFailure.dynamicCast<IOException>();
                    }
                    AMQ_CATCH_NOTHROW(ClassCastException)

                    if (ioException != NULL) {
                        transportListener->onException(*this->connectionFailure);
                    } else {
                        transportListener->onException(IOException(*this->connectionFailure));
                    }
                }
            }

            reconnectMutex.notifyAll();
        }

        void resetReconnectDelay() {
            if (!useExponentialBackOff || reconnectDelay == DEFAULT_INITIAL_RECONNECT_DELAY) {
                reconnectDelay = initialReconnectDelay;
            }
        }

        bool isClosedOrFailed() const {
            return closed.get() || connectionFailure != NULL;
        }

        void disposeTransport(Pointer<Transport>& transport) {
            Logger::trace("FailoverTransportImpl::disposeTransport("  + transport->getRemoteAddress() + ")", logCategories);
            transport->setTransportListener(this->disposedListener.get());
            closeTask->add(transport);
        }

        void disconnect() {
            Logger::trace("FailoverTrasnportImpl::disconnect()", logCategories);
            Pointer<Transport> transport;
            synchronized (&reconnectMutex) {
                transport.swap(this->connectedTransport);

                if (transport != NULL) {

                    if (this->disposedListener != NULL) {
                        transport->setTransportListener(this->disposedListener.get());
                    }

                    // Hand off to the close task so it gets done in a different thread.
                    this->closeTask->add(transport);

                    if (this->connectedTransportURI != NULL) {
                        this->connectedTransportURI.reset(NULL);
                    }
                }
            }
        }

        void reconnect(bool rebalance) {
            Logger::trace("FailoverTrasnportImpl::reconnect()", logCategories);
            Pointer<Transport> transport;

            synchronized( &reconnectMutex ) {
                if (started.get()) {

                    if (rebalance) {
                        doRebalance = true;
                    }

                    try {
                        taskRunner->wakeup();
                    } catch (InterruptedException& ex) {
                        Thread::currentThread()->interrupt();
                    }
                }
            }
        }

        Pointer<Transport> createTransport(const URI& location) const {
            Logger::trace("FailoverTrasnportImpl::createTransport(" + location.toString() + ")", logCategories);
            try {

                TransportFactory* factory = TransportRegistry::getInstance().findFactory(location.getScheme());

                if (factory == NULL) {
                    throw new IOException(__FILE__, __LINE__, "Invalid URI specified, no valid Factory Found.");
                }

                Pointer<Transport> transport(factory->createComposite(location));

                return transport;
            }
            AMQ_CATCH_RETHROW( IOException)
            AMQ_CATCH_EXCEPTION_CONVERT( Exception, IOException)
            AMQ_CATCHALL_THROW( IOException)
        }

        void restoreTransport(const Pointer<Transport> transport) {
            Logger::trace("FailoverTrasnportImpl::restoreTransport(" + transport->getRemoteAddress() + ")", logCategories);
            try {

                transport->start();

                //send information to the broker - informing it we are an ft client
                Pointer<ConnectionControl> cc(new ConnectionControl());
                cc->setFaultTolerant(true);
                transport->oneway(cc);

                stateTracker.restore(transport);

                decaf::util::StlMap<int, Pointer<Command> > commands;
                synchronized(&requestMap) {
                        commands.copy(requestMap);
                    }

                Pointer< Iterator<Pointer<Command> > > iter(commands.values().iterator());
                while (iter->hasNext()) {
                    transport->oneway(iter->next());
                }
            }
            AMQ_CATCH_RETHROW( IOException)
            AMQ_CATCH_EXCEPTION_CONVERT( Exception, IOException)
            AMQ_CATCHALL_THROW( IOException)
        }

        bool shouldBuildBackups() {
            bool shouldBuild;
            synchronized(&backupMutex) {
                shouldBuild = ((backupsEnabled && (backups.size() < backupPoolSize || backupRemoved))
                        || (priorityBackup && !(priorityBackupAvailable || connectedToPriority)))
                        && getConnectList().size() > 1 + backups.size(); // '1' - is the main/active connection
            }
            return shouldBuild;
        }

        bool doReconnect();
        bool buildBackups();

        void removeBackup();

        size_t getConnectedUriIndex(const LinkedList<URI>& connectList);
        bool alreadyBackup(Pointer<BackupTransport> transport);
        bool isBetterBackup(const LinkedList<URI>& backupList, size_t idx);
    };

    const int FailoverTransportImpl::DEFAULT_INITIAL_RECONNECT_DELAY = 10;
    const int FailoverTransportImpl::INFINITE_WAIT = -1;
}}}

////////////////////////////////////////////////////////////////////////////////
FailoverTransport::FailoverTransport() : impl(NULL) {
    this->impl = new FailoverTransportImpl(this);
    logCategories.push_back("activemq");
    logCategories.push_back("failover");
}

////////////////////////////////////////////////////////////////////////////////
FailoverTransport::~FailoverTransport() {
    Logger::trace("FailoverTrasnport::~FailoverTransport()", logCategories);
    try {
        close();
    }
    AMQ_CATCHALL_NOTHROW()

    try {
        this->impl->myTransportListener->dispose();
        delete this->impl;
    }
    AMQ_CATCHALL_NOTHROW()
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::add(bool rebalance, const std::string& uri) {

    try {
        if (this->impl->uris.add(URI(uri))) {
            reconnect(rebalance);
        }
    }
    AMQ_CATCHALL_NOTHROW()
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::addURI(bool rebalance, const List<URI>& uris) {

    bool newUri = false;

    std::auto_ptr<Iterator<URI> > iter(uris.iterator());
    while (iter->hasNext()) {
        if (this->impl->uris.add(iter->next())) {
            newUri = true;
        }
    }

    if (newUri) {
        reconnect(rebalance);
    }
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::removeURI(bool rebalance, const List<URI>& uris) {

    bool changed = false;

    std::auto_ptr<Iterator<URI> > iter(uris.iterator());
    synchronized( &this->impl->reconnectMutex ) {
        while (iter->hasNext()) {
            if (this->impl->uris.remove(iter->next())) {
                changed = true;
            }
        }
    }

    if (changed) {
        reconnect(rebalance);
    }
}

void FailoverTransport::removeBackup() {
    if (this->impl->started.get() && !this->impl->closed.get()) {
        this->impl->removeBackup();
    }
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::reconnect(const decaf::net::URI& uri) {

    try {
        if (this->impl->uris.add(uri)) {
            reconnect(true);
        }
    }
    AMQ_CATCH_RETHROW(IOException)
    AMQ_CATCH_EXCEPTION_CONVERT(Exception, IOException)
    AMQ_CATCHALL_THROW(IOException)
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setTransportListener(TransportListener* listener) {
    Logger::trace(std::string("FailoverTransport::setTransportListener(") + (listener ? "NULL" : "!NULL") + ")", logCategories);
    synchronized( &this->impl->listenerMutex ) {
        this->impl->transportListener = listener;
        this->impl->listenerMutex.notifyAll();
    }
}

////////////////////////////////////////////////////////////////////////////////
TransportListener* FailoverTransport::getTransportListener() const {
    synchronized( &this->impl->listenerMutex ) {
        return this->impl->transportListener;
    }

    return NULL;
}

////////////////////////////////////////////////////////////////////////////////
std::string FailoverTransport::getRemoteAddress() const {
    synchronized( &this->impl->reconnectMutex ) {
        if (this->impl->connectedTransport != NULL) {
            return this->impl->connectedTransport->getRemoteAddress();
        }
    }
    return "";
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::oneway(const Pointer<Command> command) {
    Logger::traceIO("FailoverTransport::oneway(" + command->toString()+ ")", logCategories);
    Pointer<Exception> error;

    try {

        synchronized(&this->impl->reconnectMutex) {
            if (command != NULL && this->impl->connectedTransport == NULL) {

                if (command->isShutdownInfo()) {
                    // Skipping send of ShutdownInfo command when not connected.
                    return;
                }

                if (command->isRemoveInfo() || command->isMessageAck()) {
                    // Simulate response to RemoveInfo command or Ack as they will be stale.
                    this->impl->stateTracker.track(command);

                    if (command->isResponseRequired()) {
                        Pointer<Response> response(new Response());
                        response->setCorrelationId(command->getCommandId());
                        this->impl->myTransportListener->onCommand(response);
                    }

                    return;
                } else if (command->isMessagePull()) {
                    // Simulate response to MessagePull if timed as we can't honor that now.
                    Pointer<MessagePull> pullRequest = command.dynamicCast<MessagePull>();
                    if (pullRequest->getTimeout() != 0) {
                        Pointer<MessageDispatch> dispatch(new MessageDispatch());
                        dispatch->setConsumerId(pullRequest->getConsumerId());
                        dispatch->setDestination(pullRequest->getDestination());
                        this->impl->myTransportListener->onCommand(dispatch);
                    }

                    return;
                }
            }

            // Keep trying until the message is sent.
            for (int i = 0; !this->impl->closed.get(); i++) {
                try {

                    // Wait for transport to be connected.
                    Pointer<Transport> transport = this->impl->connectedTransport;
                    long long start = System::currentTimeMillis();
                    bool timedout = false;

                    while (transport == NULL && !this->impl->closed.get() && this->impl->connectionFailure == NULL) {
                        long long end = System::currentTimeMillis();
                        if (command->isMessage() && this->impl->timeout > 0 && (end - start > this->impl->timeout)) {
                            timedout = true;
                            break;
                        }

                        this->impl->reconnectMutex.wait(100);
                        transport = this->impl->connectedTransport;
                    }

                    if (transport == NULL) {
                        // Previous loop may have exited due to us being disposed.
                        if (this->impl->closed.get()) {
                            error.reset(new IOException(__FILE__, __LINE__, "Transport disposed."));
                        } else if (this->impl->connectionFailure != NULL) {
                            error = this->impl->connectionFailure;
                        } else if (timedout == true) {
                            error.reset(new IOException(__FILE__, __LINE__,
                                "Failover timeout of %d ms reached.", this->impl->timeout));
                        } else {
                            error.reset(new IOException(__FILE__, __LINE__, "Unexpected failure."));
                        }

                        break;
                    }

                    // If it was a request and it was not being tracked by the state
                    // tracker, then hold it in the requestMap so that we can replay
                    // it later.
                    Pointer<Tracked> tracked;
                    try {
                        tracked = this->impl->stateTracker.track(command);
                        synchronized( &this->impl->requestMap ) {
                            if (tracked != NULL && tracked->isWaitingForResponse()) {
                                this->impl->requestMap.put(command->getCommandId(), tracked);
                            } else if (tracked == NULL && command->isResponseRequired()) {
                                this->impl->requestMap.put(command->getCommandId(), command);
                            }
                        }
                    } catch (Exception& ex) {
                        ex.setMark(__FILE__, __LINE__);
                        error.reset(ex.clone());
                        break;
                    }

                    // Send the message.
                    try {
                        Logger::traceIO("FailoverTransport::oneway - sending using underlying transport (retry count: " + Integer::toString(i) + ")", logCategories);
                        transport->oneway(command);
                        this->impl->stateTracker.trackBack(command);
                    } catch (IOException& e) {

                        e.setMark(__FILE__, __LINE__);

                        // If the command was not tracked.. we will retry in
                        // this method
                        if (tracked == NULL) {

                            // since we will retry in this method.. take it out of the
                            // request map so that it is not sent 2 times on recovery
                            if (command->isResponseRequired()) {
                                this->impl->requestMap.remove(command->getCommandId());
                            }

                            // re-throw the exception so it will handled by the outer catch
                            throw;
                        } else {
                            // Trigger the reconnect since we can't count on inactivity or
                            // other socket events to trip the failover condition.
                            handleTransportFailure(e);
                        }
                    }

                    return;
                } catch (IOException& e) {
                    e.setMark(__FILE__, __LINE__);
                    handleTransportFailure(e);
                }
            }
        }
    } catch (InterruptedException& ex) {
        Thread::currentThread()->interrupt();
        throw InterruptedIOException(__FILE__, __LINE__, "FailoverTransport oneway() interrupted");
    }
    AMQ_CATCHALL_NOTHROW()

    if (!this->impl->closed.get()) {
        if (error != NULL) {
            throw IOException(*error);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
Pointer<FutureResponse> FailoverTransport::asyncRequest(const Pointer<Command> command AMQCPP_UNUSED,
                                                        const Pointer<ResponseCallback> responseCallback AMQCPP_UNUSED) {
    throw decaf::lang::exceptions::UnsupportedOperationException(__FILE__, __LINE__, "FailoverTransport::asyncRequest - Not Supported");
}

////////////////////////////////////////////////////////////////////////////////
Pointer<Response> FailoverTransport::request(const Pointer<Command> command AMQCPP_UNUSED) {
    throw decaf::lang::exceptions::UnsupportedOperationException(__FILE__, __LINE__, "FailoverTransport::request - Not Supported");
}

////////////////////////////////////////////////////////////////////////////////
Pointer<Response> FailoverTransport::request(const Pointer<Command> command AMQCPP_UNUSED, unsigned int timeout AMQCPP_UNUSED) {
    throw decaf::lang::exceptions::UnsupportedOperationException(__FILE__, __LINE__, "FailoverTransport::request - Not Supported");
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::start() {
    Logger::log("FailoverTransport::start()", LOG_SEV_INFO, logCategories);
    try {
        if (this->impl->started.get()) {
            return;
        }

        synchronized(&this->impl->reconnectMutex) {

            this->impl->started.set(true);
            this->impl->taskRunner->start();

            this->impl->stateTracker.setMaxMessageCacheSize(this->getMaxCacheSize());
            this->impl->stateTracker.setMaxMessagePullCacheSize(this->getMaxPullCacheSize());
            this->impl->stateTracker.setTrackMessages(this->isTrackMessages());
            this->impl->stateTracker.setTrackTransactionProducers(this->isTrackTransactionProducers());

            if (this->impl->connectedTransport != NULL) {
                this->impl->stateTracker.restore(this->impl->connectedTransport);
            } else {
                reconnect(false);
            }
        }
    }
    AMQ_CATCH_RETHROW(IOException)
    AMQ_CATCH_EXCEPTION_CONVERT(Exception, IOException)
    AMQ_CATCHALL_THROW(IOException)
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::stop() {
    Logger::log("FailoverTransport::stop()", LOG_SEV_INFO, logCategories);
    try {
        this->impl->started.set(false);
    }
    AMQ_CATCH_RETHROW(IOException)
    AMQ_CATCH_EXCEPTION_CONVERT(Exception, IOException)
    AMQ_CATCHALL_THROW(IOException)
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::close() {
    Logger::log("FailoverTransport::close()", LOG_SEV_INFO, logCategories);
    try {

        if (this->impl->closed.get()) {
            return;
        }

        Pointer<Transport> transportToStop;

        synchronized(&this->impl->reconnectMutex) {

            this->impl->started.set(false);
            this->impl->closed.set(true);
            this->impl->connected = false;

            synchronized(&this->impl->backupMutex) {
                Pointer< Iterator< Pointer<BackupTransport> > > biter(this->impl->backups.iterator());
                while (biter->hasNext()) {
                    biter->next()->dispose();
                }
                this->impl->backups.clear();
                this->impl->backupMutex.notifyAll();
            }



            this->impl->requestMap.clear();

            if (this->impl->connectedTransport != NULL) {
                transportToStop.swap(this->impl->connectedTransport);
            }

            this->impl->reconnectMutex.notifyAll();
        }

        synchronized( &this->impl->sleepMutex ) {
            this->impl->sleepMutex.notifyAll();
        }

        this->impl->taskRunner->shutdown(TimeUnit::MINUTES.toMillis(5));

        if (transportToStop != NULL) {
            transportToStop->close();
        }
    }
    AMQ_CATCH_RETHROW( IOException)
    AMQ_CATCH_EXCEPTION_CONVERT( Exception, IOException)
    AMQ_CATCHALL_THROW( IOException)
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::reconnect(bool rebalance) {
    this->impl->reconnect(rebalance);
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::restoreTransport(const Pointer<Transport> transport) {
    Logger::log("FailoverTransport::restoreTransport(" + transport->getRemoteAddress() + ")", LOG_SEV_DEBUG, logCategories);
    try {

        transport->start();

        //send information to the broker - informing it we are an ft client
        Pointer<ConnectionControl> cc(new ConnectionControl());
        cc->setFaultTolerant(true);
        transport->oneway(cc);

        this->impl->stateTracker.restore(transport);

        decaf::util::StlMap<int, Pointer<Command> > commands;
        synchronized(&this->impl->requestMap) {
            commands.copy(this->impl->requestMap);
        }

        Pointer<Iterator<Pointer<Command> > > iter(commands.values().iterator());
        while (iter->hasNext()) {
            transport->oneway(iter->next());
        }
    }
    AMQ_CATCH_RETHROW( IOException)
    AMQ_CATCH_EXCEPTION_CONVERT( Exception, IOException)
    AMQ_CATCHALL_THROW( IOException)
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::handleTransportFailure(const decaf::lang::Exception& error) {
    Logger::log("FailoverTransport::handleTransportFailure(" + error.getMessage() + ")", LOG_SEV_WARNING, logCategories);
    Logger::trace("stack trace: " + error.getStackTraceString(), logCategories);
    synchronized(&this->impl->reconnectMutex) {

        Pointer<Transport> transport;
        this->impl->connectedTransport.swap(transport);

        if (transport != NULL) {

            this->impl->disposeTransport(transport);

            bool reconnectOk = this->impl->canReconnect();
            URI failedUri = *this->impl->connectedTransportURI;

            this->impl->initialized = false;
            this->impl->connectedTransportURI.reset(NULL);
            this->impl->connected = false;
            this->impl->connectedToPriority = false;

            // Place the State Tracker into a reconnection state.
            //this->impl->stateTracker.transportInterrupted();

            // Notify before we attempt to reconnect so that the consumers have a chance
            // to cleanup their state.
            if (reconnectOk) {
                synchronized(&this->impl->listenerMutex) {
                    if (this->impl->transportListener != NULL) {
                        this->impl->transportListener->transportInterrupted();
                    }
                }

                this->impl->updated.remove(failedUri);
            } else if (!this->impl->closed.get()) {
                this->impl->connectionFailure.reset(error.clone());
                this->impl->propagateFailureToExceptionListener();
            }
        }
        if (!this->impl->closed.get()) // avoid spurious wakeups
            this->impl->taskRunner->wakeup();
    }
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::handleConnectionControl(const Pointer<Command> control) {
    Logger::trace("FailoverTransport::handleConnectionControl("+ control->toString() +")", logCategories);
    try {

        Pointer<ConnectionControl> ctrlCommand = control.dynamicCast<ConnectionControl>();

        std::string reconnectStr = ctrlCommand->getReconnectTo();
        if (!reconnectStr.empty()) {

            std::remove(reconnectStr.begin(), reconnectStr.end(), ' ');

            if (reconnectStr.length() > 0) {
                try {
                    if (isReconnectSupported()) {
                        reconnect(URI(reconnectStr));
                    }
                } catch (Exception& e) {
                }
            }
        }

        processNewTransports(ctrlCommand->isRebalanceConnection(), ctrlCommand->getConnectedBrokers());
    }
    AMQ_CATCH_RETHROW( Exception)
    AMQ_CATCHALL_THROW( Exception)
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::processNewTransports(bool rebalance, std::string newTransports) {
    Logger::trace("FailoverTransport::processNewTransports("+ newTransports +")", logCategories);
    if (!newTransports.empty()) {

        std::remove(newTransports.begin(), newTransports.end(), ' ');

        if (newTransports.length() > 0 && isUpdateURIsSupported()) {

            LinkedList<URI> list;
            StringTokenizer tokenizer(newTransports, ",");

            while (tokenizer.hasMoreTokens()) {
                std::string str = tokenizer.nextToken();
                try {
                    URI uri(str);
                    list.add(uri);
                } catch (Exception& e) {
                }
            }

            if (!list.isEmpty()) {
                try {
                    updateURIs(rebalance, list);
                } catch (IOException& e) {
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::updateURIs(bool rebalance, const decaf::util::List<decaf::net::URI>& updatedURIs) {

    if (isUpdateURIsSupported()) {
        this->impl->updated.clear();

        if (!updatedURIs.isEmpty()) {
            // unique
            StlSet<URI> set;
            for (int i = 0; i < updatedURIs.size(); i++) {
                set.add(updatedURIs.get(i));
            }

            this->impl->updated.addAll(set);

            this->impl->buildBackups(); // TODO
            reconnect(rebalance);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isPending() const {
    synchronized(&this->impl->reconnectMutex) {
        synchronized(&this->impl->backupMutex) {
            if ((this->impl->connectedTransport == NULL
                 || this->impl->doRebalance
                 || this->impl->priorityBackupAvailable
                 || this->impl->shouldBuildBackups()
                 ) && this->impl->started.get() && !this->impl->closed.get()) {
                Logger::trace("isPending returned true", logCategories);
                return true;
            }
        }
    }
    Logger::trace("isPending returned false", logCategories);
    return false;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::iterate() {
    Logger::log("FailoverTransport::iterate()", LOG_SEV_DEBUG, logCategories);
    bool result = false;
    if (!this->impl->started.get()) {
        return result;
    }

    bool buildBackup = true;
    synchronized(&this->impl->reconnectMutex) {
        synchronized(&this->impl->backupMutex) {
            if ((this->impl->connectedTransport == NULL || this->impl->doRebalance || this->impl->priorityBackupAvailable) && !this->impl->closed.get()) {
                result = this->impl->doReconnect();
                buildBackup = false;
            }
        }
    }
    if (buildBackup) {
        this->impl->buildBackups();
        if (this->impl->priorityBackup && !this->impl->connectedToPriority) {
            try {
                this->impl->doDelay();
                this->impl->taskRunner->wakeup();
            } catch (Exception &e) {
            }
        }
    } else {
        try {
            this->impl->doDelay();
            this->impl->taskRunner->wakeup();
        } catch (Exception &e) {
        }
    }
    return result;
}

bool FailoverTransportImpl::doReconnect() {
    Logger::log("FailoverTransportImpl::doReconnect()", LOG_SEV_DEBUG, logCategories);
    Pointer<Exception> failure;

    synchronized( &reconnectMutex ) {
        if (closed.get() || connectionFailure != NULL) {
            reconnectMutex.notifyAll();
        }
        if ((connectedTransport != NULL && !doRebalance && !priorityBackupAvailable) || closed.get() || connectionFailure != NULL) {
            return false;
        } else {
            List<URI>& connectList = getConnectList();
            if (connectList.isEmpty()) {
                failure.reset(new IOException(__FILE__, __LINE__, "No URIs available for reconnect."));
            } else {
                if (doRebalance) {
                    if (connectedToPriority || connectList.get(0) == *connectedTransportURI) {
                        doRebalance = false;
                        return false;
                    } else {
                        try {
                            Pointer<Transport> transport;
                            this->connectedTransport.swap(transport);
                            if (transport != NULL) {
                                disposeTransport(transport);
                            }
                        } catch (Exception& e) {}
                    }
                    doRebalance = false;
                }

                resetReconnectDelay();

                Pointer<Transport> transport;
                URI uri;

                synchronized (&backupMutex) {
                    if ((priorityBackup || backupsEnabled) &&!backups.isEmpty()) {
                        Pointer<BackupTransport> bt = backups.get(0);
                        backups.remove(bt);
                        transport = bt->getTransport();
                        transport->setTransportListener(myTransportListener.get());
                        uri = bt->getUri();
                        Logger::log("Using backup transport " + uri.toString(), LOG_SEV_INFO, logCategories);
                        if (priorityBackup && priorityBackupAvailable) {
                            Pointer<Transport> oldTransport;
                            this->connectedTransport.swap(oldTransport);
                            this->connectedTransportURI.reset(new URI(uri));
                            if (oldTransport != NULL) {
                                disposeTransport(oldTransport);
                            }
                            priorityBackupAvailable = false;
                        }
                    }
                }

                if (transport == NULL && !firstConnection && (reconnectDelay > 0) && !closed.get()) {
                    synchronized(&sleepMutex) {
                        sleepMutex.wait(reconnectDelay);
                    }
                }

                Pointer< Iterator<URI> > iter(connectList.iterator());
                while ((transport != NULL || iter->hasNext()) && (connectedTransport == NULL && !closed.get())) {
                    try {
                        if (transport == NULL) {
                            uri = iter->next();
                            transport = createTransport(uri);
                        }
                        Logger::log("Connecting to " + uri.toString(), LOG_SEV_DEBUG, logCategories);

                        transport->setTransportListener(myTransportListener.get());
                        transport->start();

                        if (started.get() && !firstConnection) {
                            restoreTransport(transport);
                        }

                        reconnectDelay = initialReconnectDelay;
                        connectedTransportURI.reset(new URI(uri));
                        connectedTransport = transport;
                        Logger::log("FailoverTransport connected to " + connectedTransportURI->toString(), LOG_SEV_INFO, logCategories);
                        connectedToPriority = isPriority(uri);

                        reconnectMutex.notifyAll();
                        connectFailures = 0;

                        // Make sure on initial startup, that the transportListener
                        // has been initialized for this instance.
                        synchronized(&listenerMutex) {
                            if (transportListener == NULL) {
                                // if it isn't set after 2secs - it probably never will be
                                listenerMutex.wait(2000);
                            }
                        }

                        synchronized(&listenerMutex) {
                            if (transportListener != NULL) {
                                transportListener->transportResumed();
                            }
                        }

                        if (firstConnection) {
                            firstConnection = false;
                        }

                        connected = true;
                        return false;
                    } catch (Exception& e) {
                        e.setMark(__FILE__, __LINE__);
                        Logger::log("FailoverTransport failed to connect to " + uri.toString(), LOG_SEV_INFO, logCategories);
                        failure.reset(e.clone());
                        if (transport != NULL) {
                            try {
                                transport->stop();
                                disposeTransport(transport);
                            } catch (...) {
                            }
                        }
                    }
                }
            }
        }

        int reconnectAttempts = calculateReconnectAttemptLimit();

        if (reconnectAttempts >= 0 && ++connectFailures >= reconnectAttempts) {
            connectionFailure = failure;

            // Make sure on initial startup, that the transportListener has been initialized
            // for this instance.
            synchronized(&listenerMutex) {
                if (transportListener == NULL) {
                    listenerMutex.wait(2000);
                }
            }

            propagateFailureToExceptionListener();
            return false;
        }
    }

    if (!closed.get()) {
        doDelay();
    }

    return !closed.get();
}

void FailoverTransportImpl::removeBackup() {
    synchronized(&backupMutex) {
        backupRemoved = true;
        taskRunner->wakeup();
    }
}

bool FailoverTransportImpl::buildBackups() {
    Logger::log("FailoverTransportImpl::buildBackups()", LOG_SEV_DEBUG, logCategories);

    if (!closed.get() && shouldBuildBackups()) {
        LinkedList<URI> backupList = priorityList;
        LinkedList<URI> connectList = getConnectList();
        {
            Pointer<Iterator<URI> > iter(connectList.iterator());
            while (iter->hasNext()) {
                URI u = iter->next();
                if (!backupList.contains(u)) {
                    backupList.add(u);
                }
            }
        }

        synchronized (&backupMutex) {
            LinkedList<Pointer<BackupTransport> > disposedList;
            Pointer< Iterator< Pointer<BackupTransport> > > biter(backups.iterator());
            while (biter->hasNext()) {
                Pointer<BackupTransport> bt = biter->next();
                if (bt->isClosed()) {
                    Logger::log("Removing backup transport " + bt->getUri().toString(), LOG_SEV_INFO, logCategories);
                    bt->dispose();
                    disposedList.add(bt);
                }
            }
            backups.removeAll(disposedList);
            disposedList.clear();
            backupRemoved = false;
        }

        size_t connectedIdx = getConnectedUriIndex(connectList);
        size_t i = 0;
        for (Pointer< Iterator<URI> > iter(backupList.iterator()); !closed.get() && iter->hasNext() && shouldBuildBackups(); ) {
            URI uri = iter->next();
            bool differentURI;
            synchronized (&reconnectMutex) {
                differentURI = connectedTransportURI != NULL && !(*connectedTransportURI == uri);
            }

            if (differentURI) {
                synchronized(&backupMutex) {
                    try {
                        Pointer<BackupTransport> bt(new BackupTransport(parent));
                        bt->setUri(uri);

                        if (!alreadyBackup(bt)) {
                            bool betterBackup = isBetterBackup(backupList, i);
                            bool highPriority = (priorityBackup && isPriority(uri)) || (autoPriority && (i < connectedIdx || betterBackup));
                            bool poolNotFull = backups.size() < backupPoolSize;

                            Pointer<Transport> t;
                            if (highPriority || poolNotFull) {
                                Logger::trace("Building backup: " + uri.toString(), logCategories);
                                try {
                                    t = createTransport(uri);
                                    t->setTransportListener(bt.get());
                                    t->start();
                                    bt->setTransport(t);
                                } catch (Exception& ex) {
                                    if (t != NULL) {
                                        bt->setTransport(Pointer<Transport>());
                                        disposeTransport(t);
                                    }
                                    continue;
                                }
                            }

                            if (highPriority) {
                                priorityBackupAvailable = i < connectedIdx;
                                backups.add(0, bt);
                                // if this priority backup overflows the pool
                                // remove the backup with the lowest priority
                                if (backups.size() > backupPoolSize) {
                                    Pointer<BackupTransport> obsolete = backups.get(backups.size() - 1);
                                    Pointer<Transport> transport = obsolete->getTransport();
                                    backups.remove(obsolete);
                                    obsolete->dispose();
                                    Logger::trace("Backup pool overflow - removing " + transport->getRemoteAddress(), logCategories);
                                    if (transport != NULL) {
                                        disposeTransport(transport);
                                    }
                                }
                            } else if (poolNotFull) {
                                backups.add(bt);
                            }
                        }
                    } catch (Exception& e) {
                        Logger::log("Failed building backup for: " + uri.toString(), LOG_SEV_DEBUG, logCategories);
                    }
                }
            }
            ++i;
        }
    }
    return false;
}

size_t FailoverTransportImpl::getConnectedUriIndex(const LinkedList<URI>& connectList) {
    size_t idx = 0;
    Pointer<Iterator<URI> > iter(connectList.iterator());
    synchronized( &reconnectMutex ) {
        while (iter->hasNext()) {
            URI uri = iter->next();
            if (connectedTransportURI != NULL && *connectedTransportURI == uri) {
                break;
            }
            ++idx;
        }
    }
    return idx;
}

bool FailoverTransportImpl::alreadyBackup(Pointer<BackupTransport> transport) {
    Pointer< Iterator< Pointer<BackupTransport> > > iter(backups.iterator());
    while (iter->hasNext()) {
        URI uri = iter->next()->getUri();
        if (uri == transport->getUri()) {
            return true;
        }
    }
    return false;
}

bool FailoverTransportImpl::isBetterBackup(const LinkedList<URI>& backupList, size_t idx) {
    Pointer< Iterator< Pointer<BackupTransport> > > iter(backups.iterator());
    while (iter->hasNext()) {
        URI uri = iter->next()->getUri();
        size_t backupIdx = static_cast<size_t>(backupList.indexOf(uri));
        if (backupIdx < idx) {
            return false;
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////
Pointer<Transport> FailoverTransport::createTransport(const URI& location) const {
    Logger::trace("FailoverTransport::createTransport(" + location.toString() + ")", logCategories);
    try {

        TransportFactory* factory = TransportRegistry::getInstance().findFactory(location.getScheme());

        if (factory == NULL) {
            throw new IOException(__FILE__, __LINE__, "Invalid URI specified, no valid Factory Found.");
        }

        Pointer<Transport> transport(factory->createComposite(location));

        return transport;
    }
    AMQ_CATCH_RETHROW( IOException)
    AMQ_CATCH_EXCEPTION_CONVERT( Exception, IOException)
    AMQ_CATCHALL_THROW( IOException)
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setConnectionInterruptProcessingComplete(const Pointer<commands::ConnectionId> connectionId) {
    Logger::trace("FailoverTransport::setConnectionInterruptProcessingComplete(" + connectionId->toString() + ")", logCategories);
    synchronized(&this->impl->reconnectMutex) {
        this->impl->stateTracker.connectionInterruptProcessingComplete(this, connectionId);
    }
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isConnected() const {
    return this->impl->connected;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isClosed() const {
    return this->impl->closed.get();
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isInitialized() const {
    return this->impl->initialized;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setInitialized(bool value) {
    this->impl->initialized = value;
}

////////////////////////////////////////////////////////////////////////////////
Transport* FailoverTransport::narrow(const std::type_info& typeId) {

    if (typeid( *this ) == typeId) {
        return this;
    }

    synchronized (&this->impl->reconnectMutex) {
        if (this->impl->connectedTransport != NULL) {
            return this->impl->connectedTransport->narrow(typeId);
        }
    }

    return NULL;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::processResponse(const Pointer<Response> response) {
    Logger::traceIO("FailoverTransport::processResponse(" + response->toString() + ")", logCategories);
    Pointer<Command> object;

    synchronized(&(this->impl->requestMap)) {
        try {
            object = this->impl->requestMap.remove(response->getCorrelationId());
        } catch (NoSuchElementException& ex) {
            // Not tracking this request in our map, not an error.
        }
    }

    if (object != NULL) {
        try {
            Pointer<Tracked> tracked = object.dynamicCast<Tracked>();
            tracked->onResponse();
        }
        AMQ_CATCH_NOTHROW( ClassCastException)
    }
}

void FailoverTransport::forwardCommand(const Pointer<Command> command) {
    synchronized(&this->impl->listenerMutex) {
        if (this->impl->transportListener != NULL) {
            this->impl->transportListener->onCommand(command);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
Pointer<wireformat::WireFormat> FailoverTransport::getWireFormat() const {

    Pointer<wireformat::WireFormat> result;
    synchronized (&this->impl->reconnectMutex) {
        Pointer<Transport> transport = this->impl->connectedTransport;

        if (transport != NULL) {
            result = transport->getWireFormat();
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////
long long FailoverTransport::getTimeout() const {
    return this->impl->timeout;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setTimeout(long long value) {
    this->impl->timeout = value;
}

////////////////////////////////////////////////////////////////////////////////
long long FailoverTransport::getInitialReconnectDelay() const {
    return this->impl->initialReconnectDelay;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setInitialReconnectDelay(long long value) {
    this->impl->initialReconnectDelay = value;
}

////////////////////////////////////////////////////////////////////////////////
long long FailoverTransport::getMaxReconnectDelay() const {
    return this->impl->maxReconnectDelay;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setMaxReconnectDelay(long long value) {
    this->impl->maxReconnectDelay = value;
}

////////////////////////////////////////////////////////////////////////////////
long long FailoverTransport::getBackOffMultiplier() const {
    return this->impl->backOffMultiplier;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setBackOffMultiplier(long long value) {
    this->impl->backOffMultiplier = value;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isUseExponentialBackOff() const {
    return this->impl->useExponentialBackOff;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setUseExponentialBackOff(bool value) {
    this->impl->useExponentialBackOff = value;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isRandomize() const {
    return this->impl->randomize;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setRandomize(bool value) {
    this->impl->randomize = value;
}

////////////////////////////////////////////////////////////////////////////////
int FailoverTransport::getMaxReconnectAttempts() const {
    return this->impl->maxReconnectAttempts;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setMaxReconnectAttempts(int value) {
    this->impl->maxReconnectAttempts = value;
}

////////////////////////////////////////////////////////////////////////////////
int FailoverTransport::getStartupMaxReconnectAttempts() const {
    return this->impl->startupMaxReconnectAttempts;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setStartupMaxReconnectAttempts(int value) {
    this->impl->startupMaxReconnectAttempts = value;
}

////////////////////////////////////////////////////////////////////////////////
long long FailoverTransport::getReconnectDelay() const {
    return this->impl->reconnectDelay;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setReconnectDelay(long long value) {
    this->impl->reconnectDelay = value;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isBackup() const {
    return this->impl->backupsEnabled;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setBackup(bool value) {
    this->impl->backupsEnabled = value;
}

////////////////////////////////////////////////////////////////////////////////
int FailoverTransport::getBackupPoolSize() const {
    return this->impl->backupPoolSize;
    // return this->impl->backups->getBackupPoolSize();
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setBackupPoolSize(int value) {
    this->impl->backupPoolSize = value;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isTrackMessages() const {
    return this->impl->trackMessages;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setTrackMessages(bool value) {
    this->impl->trackMessages = value;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isTrackTransactionProducers() const {
    return this->impl->trackTransactionProducers;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setTrackTransactionProducers(bool value) {
    this->impl->trackTransactionProducers = value;
}

////////////////////////////////////////////////////////////////////////////////
int FailoverTransport::getMaxCacheSize() const {
    return this->impl->maxCacheSize;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setMaxCacheSize(int value) {
    this->impl->maxCacheSize = value;
}

////////////////////////////////////////////////////////////////////////////////
int FailoverTransport::getMaxPullCacheSize() const {
    return this->impl->maxPullCacheSize;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setMaxPullCacheSize(int value) {
    this->impl->maxPullCacheSize = value;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isReconnectSupported() const {
    return this->impl->reconnectSupported;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setReconnectSupported(bool value) {
    this->impl->reconnectSupported = value;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isUpdateURIsSupported() const {
    return this->impl->updateURIsSupported;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setUpdateURIsSupported(bool value) {
    this->impl->updateURIsSupported = value;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isRebalanceUpdateURIs() const {
    return this->impl->rebalanceUpdateURIs;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setRebalanceUpdateURIs(bool rebalanceUpdateURIs) {
    this->impl->rebalanceUpdateURIs = rebalanceUpdateURIs;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isPriorityBackup() const {
    return this->impl->priorityBackup;
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setPriorityBackup(bool priorityBackup) {
    this->impl->priorityBackup = priorityBackup;
}

////////////////////////////////////////////////////////////////////////////////
bool FailoverTransport::isConnectedToPriority() const {
    return this->impl->connectedToPriority;
}

bool FailoverTransport::isAutoPriority() const {
    return this->impl->autoPriority;
}

void FailoverTransport::setAutoPriority(bool autoPriority) {
    this->impl->autoPriority = autoPriority;
    if (autoPriority) {
        this->impl->backupsEnabled = true;
        this->impl->priorityBackup = true;
    }
}

////////////////////////////////////////////////////////////////////////////////
void FailoverTransport::setPriorityURIs(const std::string& priorityURIs AMQCPP_UNUSED) {
    StringTokenizer tokenizer(priorityURIs, ",");
    this->impl->priorityList.clear();
    while (tokenizer.hasMoreTokens()) {
        std::string str = tokenizer.nextToken();
        try {
            this->impl->priorityList.add(URI(str));
        } catch (Exception& e) {
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
const List<URI>& FailoverTransport::getPriorityURIs() const {
    return this->impl->priorityList;
}
