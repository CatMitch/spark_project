#!/bin/sh

env=$1
filePath=$2
fileRights=$3

echo $env $filePath $fileRights

if  [[ $env = jupyter ]]; 
then
    filePath="$filePath"
    fileRights="$fileRights"
else 
	read -erp "Path   [default: ./files]: " filePath && filePath=${filePath:-./files}
    read -erp "Rights [default 750]:" fileRights && fileRights=${fileRights:-750}
fi

. parse_yaml.sh
. fileCreate.sh

# read yaml file
eval $(parse_yaml parameters.yml "parameter_")

# access yaml content
echo $parameter_url_prices

# create a directory with the given options from the user
fileCreate $filePath $fileRights
