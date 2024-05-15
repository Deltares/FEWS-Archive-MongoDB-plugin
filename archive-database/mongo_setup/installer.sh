#!/bin/bash

while true; do
	read -p "Would you like to [i]nstall or [r]emove mongodb: " operation
	case $operation in
		[Ii]* ) 
			read -e -p "Install directory: " -i "/opt/FEWSArchiveMongoDB" install_dir
			read -e -p "Data Directory: " -i "/var/lib/FEWSArchiveMongoDB" data_dir
			read -e -p "Log Directory: " -i "/var/log/FEWSArchiveMongoDB" log_dir
			read -e -p "Mongo Service Name: " -i "MongoDB" service_name
			read -e -p "Port Number: " -i 27017 port
			sudo bash ./mongo_setup.sh install --install_dir $install_dir --data_dir $data_dir --log_dir $log_dir --service_name $service_name --port $port
			break;;
		[Rr]* ) 
			read -e -p "Install directory: " -i "/opt/FEWSArchiveMongoDB" install_dir
			read -e -p "Mongo Service Name: " -i "MongoDB" service_name
			sudo bash ./mongo_setup.sh remove --install_dir $install_dir --service_name $service_name
			break;;
		* ) echo "Please select [i]nstall or [r]emove"
	esac
done
