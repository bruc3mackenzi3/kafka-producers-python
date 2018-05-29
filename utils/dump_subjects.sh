#!/bin/bash

# Dev schema registry server
if [[ $1 == "-l" || $1 == "--location" ]]
then
    if [[ $2 == "dev" ]]
    then
        location="10.42.32.102:8081"
    elif [[ $2 == "prod" ]]
    then
        location="10.42.45.251:8081"
    else
        echo "Error: argument to $1 must be one of dev, prod."
        exit 1
    fi
elif [[ $1 == "-h" || $1 == "--help" ]]
then
    echo "Usage:\n\t./utils/dump_dubjects.sh [--location dev|prod]"
else
    echo "No location argument provided, defaulting to production."
    location="10.42.45.251:8081"
fi

tmp_dir="schemas_$(date +%d-%m-%y)"
mkdir $tmp_dir

schemas=$(curl --silent http://$location/subjects)
for schema in $(echo $schemas | jq --raw-output .[])
do
    curl --silent http://$location/subjects/$schema/versions/latest/schema > $tmp_dir/$schema
done

echo "Done.  Schemas available in $tmp_dir"
