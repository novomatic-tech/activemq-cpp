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

#include "BackupTransport.h"


using namespace activemq;
using namespace activemq::transport;
using namespace activemq::transport::failover;

////////////////////////////////////////////////////////////////////////////////
BackupTransport::BackupTransport(FailoverTransport* parent) :
    parent(parent), transport(), uri(), closed(true), priority(false) {
    logCategories.push_back("activemq");
    logCategories.push_back("failover");
    logCategories.push_back("backup");
}

////////////////////////////////////////////////////////////////////////////////
BackupTransport::~BackupTransport() {
    dispose();
}

////////////////////////////////////////////////////////////////////////////////
void BackupTransport::onException(const decaf::lang::Exception& ex) {
    FailoverTransport* p = NULL;
    synchronized(&mutex) {
        this->closed = true;

        Logger::log("Backup transport error: " + ex.getMessage(), LOG_SEV_DEBUG, logCategories);

        if (this->parent != NULL) {
            p = this->parent;
        }
    }

    if (p != NULL) {
        p->removeBackup();
    }
}
