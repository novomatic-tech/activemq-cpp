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

#include "CloseTransportsTask.h"

#include <activemq/exceptions/ActiveMQException.h>

#include <activemq/util/Logger.h>

using namespace activemq;
using namespace activemq::threads;
using namespace activemq::exceptions;
using namespace activemq::transport;
using namespace activemq::transport::failover;
using namespace activemq::util;
using namespace LightBridge::Middleware::Logger;
using namespace decaf;
using namespace decaf::lang;
using namespace decaf::util;
using namespace decaf::util::concurrent;

////////////////////////////////////////////////////////////////////////////////
CloseTransportsTask::CloseTransportsTask() :
    transports() {
    logCategories.push_back("activemq");
    logCategories.push_back("failover");
}

////////////////////////////////////////////////////////////////////////////////
CloseTransportsTask::~CloseTransportsTask() {

}

////////////////////////////////////////////////////////////////////////////////
void CloseTransportsTask::add(Pointer<Transport>& transport) {
    synchronized (&transports) {
        transports.push(transport);
        transport.reset(NULL);
    };
}

////////////////////////////////////////////////////////////////////////////////
bool CloseTransportsTask::isPending() const {
    bool pending = false;
    synchronized (&transports) {
        pending = !transports.empty();
    };
    return pending;
}

////////////////////////////////////////////////////////////////////////////////
bool CloseTransportsTask::iterate() {
    Pointer<Transport> transport;
    synchronized (&transports) {
         while (!transports.empty()) {
             transport = transports.pop();
             if (transport != NULL) {

                 try {
                     Logger::log("Closing transport " + transport->getRemoteAddress(), LOG_SEV_DEBUG, logCategories);
                     transport->close();
                 }
                 AMQ_CATCHALL_NOTHROW()

                 transport.reset(NULL);
             }
         }
    }



    return isPending();
}
