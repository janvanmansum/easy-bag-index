#!/usr/bin/env bash
#
# Copyright (C) 2017 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


NUMBER_OF_INSTALLATIONS=$1
MODULE_NAME=easy-bag-index
MODULE_USER=${MODULE_NAME}
DATABASE_NAME=easy_bag_index
INSTALL_DIR=/opt/dans.knaw.nl/${MODULE_NAME}
LOG_DIR=/var/opt/dans.knaw.nl/log/${MODULE_NAME}
INITD_SCRIPTS_DIR=/etc/init.d
SYSTEMD_SCRIPTS_DIR=/usr/lib/systemd/system

echo "POST-INSTALL: START (Number of current installations: $NUMBER_OF_INSTALLATIONS)"

if [ ${NUMBER_OF_INSTALLATIONS} -eq 1 ]; then # First install
    echo "First time install, replacing default config with RPM-aligned one"
    #
    # Temporary arrangement to make sure the default config settings align with the FHS-abiding
    # RPM installation
    #
    rm /etc/opt/dans.knaw.nl/${MODULE_NAME}/logback-service.xml
    mv /etc/opt/dans.knaw.nl/${MODULE_NAME}/rpm-logback-service.xml /etc/opt/dans.knaw.nl/${MODULE_NAME}/logback-service.xml


    sudo -u postgres psql -c "\q" ${DATABASE_NAME} 2> /dev/null
    if [ $? -ne 0 ]; then
        echo "Creating database..."
        sudo -u postgres psql -c "CREATE ROLE $DATABASE_NAME WITH LOGIN PASSWORD 'changeme'"
        sudo -u postgres psql -c "CREATE DATABASE $DATABASE_NAME WITH OWNER = $DATABASE_NAME ENCODING = 'UTF8' CONNECTION LIMIT = -1"
        sudo -u postgres psql -d ${DATABASE_NAME} -f ${INSTALL_DIR}/bin/db-tables.sql
        echo "Database created. DO NOT FORGET TO CHANGE YOUR ADMIN PASSWORD FROM THE DEFAULT TO SOMETHING SAFE!"
    else
        echo "Database $DATABASE_NAME already exists. Please, remove database before reinstalling."
        exit 1
    fi
fi

if [ -d ${INITD_SCRIPTS_DIR} ]; then
    echo -n "Installing initd service script... "
    cp ${INSTALL_DIR}/bin/${MODULE_NAME}-initd.sh ${INITD_SCRIPTS_DIR}/${MODULE_NAME}
    chmod o+x ${INITD_SCRIPTS_DIR}/${MODULE_NAME}
    echo "OK"
fi

if [ -d ${SYSTEMD_SCRIPTS_DIR} ]; then
    echo -n "Installing systemd service script... "
    cp ${INSTALL_DIR}/bin/${MODULE_NAME}.service ${SYSTEMD_SCRIPTS_DIR}/
    echo "OK"
fi

if [ ! -d ${LOG_DIR} ]; then
    echo -n "Creating directory for logging... "
    mkdir -p ${LOG_DIR}
    chown ${MODULE_USER} ${LOG_DIR}
    echo "OK"
fi

echo "POST-INSTALL: DONE."
