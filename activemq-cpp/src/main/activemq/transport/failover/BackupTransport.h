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

#ifndef _ACTIVE_TRANSPORT_FAILOVER_BACKUPTRANSPORT_H_
#define _ACTIVE_TRANSPORT_FAILOVER_BACKUPTRANSPORT_H_

#include <decaf/util/concurrent/Mutex.h>
#include <activemq/util/Config.h>
#include <activemq/transport/Transport.h>
#include <activemq/transport/DefaultTransportListener.h>
#include <decaf/net/URI.h>
#include <decaf/lang/Pointer.h>
#include <memory>
#include "FailoverTransport.h"

namespace activemq {
namespace transport {
namespace failover {

    using decaf::lang::Pointer;
    using decaf::util::concurrent::Mutex;

    class AMQCPP_API BackupTransport : public DefaultTransportListener {
    private:

        Mutex mutex;

        // The parent of this Backup
        FailoverTransport* parent;

        // The Transport this one is managing.
        Pointer<Transport> transport;

        // The URI of this Backup
        decaf::net::URI uri;

        // Indicates that the contained transport is not valid any longer.
        bool closed;

        // Is this Transport one of the priority backups.
        bool priority;

        std::vector<std::string> logCategories;

    private:

        BackupTransport(const BackupTransport&);
        BackupTransport& operator=(const BackupTransport&);

    public:

        BackupTransport(FailoverTransport* failover = NULL);

        virtual ~BackupTransport();

        /**
         * Gets the URI assigned to this Backup
         * @return the assigned URI
         */
        decaf::net::URI getUri() const {
            return this->uri;
        }

        /**
         * Sets the URI assigned to this Transport.
         */
        void setUri(const decaf::net::URI& uri) {
            this->uri = uri;
        }

        /**
         * Gets the currently held transport
         * @returns pointer to the held transport or NULL if not set.
         */
        const Pointer<Transport>& getTransport() {
            synchronized(&mutex) {
                return transport;
            }
        }

        /**
         * Sets the held transport, if not NULL then start to listen for exceptions
         * from the held transport.
         *
         * @param transport
         *        The transport to hold.
         */
        void setTransport(const Pointer<Transport> transport) {
            synchronized(&mutex) {
                this->transport = transport;

                if (this->transport != NULL) {
                    this->transport->setTransportListener(this);
                    setClosed(false);
                }
            }
        }

        /**
         * Event handler for an exception from a command transport.
         * <p>
         * The BackupTransport closes its internal Transport when an exception is
         * received and returns the URI to the pool of URIs to attempt connections to.
         *
         * @param ex
         *      The exception that was passed to this listener to handle.
         */
        virtual void onException(const decaf::lang::Exception& ex);

        /**
         * Has the Transport been shutdown and no longer usable.
         *
         * @returns true if the Transport
         */
        bool isClosed() const {
            return this->closed;
        }

        /**
         * Sets the closed flag on this Transport.
         * @param value - true for closed.
         */
        void setClosed(bool value) {
            this->closed = value;
        }

        /**
         * @returns true if this transport was in the priority backup list.
         */
        bool isPriority() const {
            return this->priority;
        }

        /**
         * Set if this transport is a Priority backup or not.
         *
         * @param value
         *      True if this is a priority backup.
         */
        void setPriority(bool value) {
            this->priority = value;
        }

        void dispose() {
            synchronized(&mutex) {
                closed = true;
                this->parent = NULL;
                if (transport != NULL) {
                    transport->setTransportListener(NULL);
                    transport.reset(NULL);
                }
            }
        }
    };

}}}

#endif /* _ACTIVE_TRANSPORT_FAILOVER_BACKUPTRANSPORT_H_ */
