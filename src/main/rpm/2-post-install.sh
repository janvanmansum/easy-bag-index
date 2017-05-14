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
echo "Executing POST-INSTALL. Number of current installations: $NUMBER_OF_INSTALLATIONS"

INSTALL_DIR=/opt/dans.knaw.nl/easy-bag-index
LOGDIR=/var/opt/dans.knaw.nl/log/easy-bag-index
INITD_SCRIPTS=/etc/init.d
SYSTEMD_SCRIPTS=/usr/lib/systemd/system
BAG_INDEX_USER=easy-bag-index

if [ $NUMBER_OF_INSTALLATIONS -eq 1 ]; then # First install
    echo "First time install, replacing default config with RPM-aligned one"
    #
    # Temporary arrangement to make sure the default config settings align with the FHS-abiding
    # RPM installation
    #
    rm /etc/opt/dans.knaw.nl/easy-bag-store/logback-service.xml
    mv /etc/opt/dans.knaw.nl/easy-bag-store/rpm-logback-service.xml /etc/opt/dans.knaw.nl/easy-bag-store/logback-service.xml


    sudo -u postgres psql -c "\q" easy_bag_index 2> /dev/null
    if [ $? -ne 0 ]; then
        echo "Creating database..."
        sudo -u postgres psql -c "CREATE ROLE easy_bag_index WITH LOGIN PASSWORD 'changeme'"
        sudo -u postgres psql -c "CREATE DATABASE easy_bag_index WITH OWNER = easy_bag_index ENCODING = 'UTF8' CONNECTION LIMIT = -1"
        sudo -u postgres psql -d easy_bag_index -f $INSTALL_DIR/bin/db-tables.sql
        echo "Database created. DO NOT FORGET TO CHANGE YOUR ADMIN PASSWORD FROM THE DEFAULT TO SOMETHING SAFE!"
    else
        echo "Database easy_bag_index already exists. Please, remove database before reinstalling."
        exit 1
    fi
fi

if [ ! -d $LOGDIR ]; then
    mkdir -p $LOGDIR
    chown $BAG_INDEX_USER $LOGDIR
fi

if [ -d $INITD_SCRIPTS ]; then
    cp $INSTALL_DIR/bin/easy-bag-index-initd.sh $INITD_SCRIPTS/easy-bag-index
fi

if [ -d $SYSTEMD_SCRIPTS ]; then
    cp $INSTALL_DIR/bin/easy-bag-index.service $SYSTEMD_SCRIPTS/
fi


