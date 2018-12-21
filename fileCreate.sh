#!/bin/sh

fileCreate() {

    filePath=$1
    fileRights=$2

    mkdir "$filePath"

    # save the github files to a local csv
    for ((i=$parameter_url_prices_year_started;i<=$parameter_url_prices_year_ended;i++));
    do
    	wget -O $filePath/prices_$i.zip $parameter_url_prices_root$parameter_url_prices_$i.zip
    done

    wget -O $filePath/stations.zip $parameter_url_stations
    wget -O $filePath/services.zip $parameter_url_services

    chmod $fileRights $filePath

    echo "Created directory at $filePath, with rights $fileRights"
}