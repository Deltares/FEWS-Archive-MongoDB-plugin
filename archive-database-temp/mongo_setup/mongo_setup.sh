#!/bin/bash

operation=$1
params=$*
while [ -n "$2" ]
do
	t=$2
	eval ${t:2}=$3
	shift
	shift
done

case $operation in
	install)
		if [ -z "$install_dir" ] || [ -z "$data_dir" ] || [ -z "$log_dir" ] || [ -z "$service_name" ] || [ -z "$port" ]; then
			echo "sudo bash ./mongo_setup.sh install --install_dir install_dir --data_dir data_dir --log_dir log_dir --service_name service_name --port port"
			echo "install_dir=$install_dir"
			echo "data_dir=$data_dir"
			echo "log_dir=$log_dir"
			echo "service_name=$service_name"
			echo "port=$port"
		else 
			if ! id $service_name >/dev/null 2>&1; then
				echo "Creating User..."
				useradd --system --no-create-home $service_name
			fi

			echo "Creating Directories..."
			mkdir -p $install_dir
			mkdir -p $data_dir
			mkdir -p $log_dir


			chown -R $service_name:$service_name $install_dir
			chown -R $service_name:$service_name $data_dir
			chown -R $service_name:$service_name $log_dir
			
			if (systemctl -q is-active $service_name);then
				echo "Stopping Service..."
				sudo systemctl stop $service_name
			fi

			if [ -f /lib/systemd/system/$service_name.service ]; then
				echo "Removing service..."
				systemctl disable $service_name
				rm /lib/systemd/system/$service_name.service
			fi

			echo "Installing MongoDB..."
			apt-get -qq install libcurl4 openssl liblzma5
			tar -zxf mongodb-linux-x86_64-ubuntu2004-4.4.3.tgz -C $install_dir
			cp -ar $install_dir/mongodb-linux-x86_64-ubuntu2004-4.4.3/* $install_dir/
			rm -fr $install_dir/mongodb-linux-x86_64-ubuntu2004-4.4.3
			rm -f $install_dir/bin/install_compass


			echo "Writing Configuration..."
			cat > /etc/$service_name.conf << EOL
storage:
 dbPath: $data_dir
 journal:
   enabled: true
systemLog:
 destination: "file"
 path: $log_dir/$service_name.log
net:
 bindIp: 0.0.0.0
 port: $port
security:
 authorization: "disabled"
EOL
		
			cat > /lib/systemd/system/$service_name.service << EOL
[Unit]
Description=MongoDB Server ($service_name)
Documentation=https://docs.mongodb.org/manual
After=network-online.target
Wants=network-online.target

[Service]
User=$service_name
Group=$service_name
EnvironmentFile=-/etc/default/$service_name
ExecStart=$install_dir/bin/mongod --config /etc/$service_name.conf
PIDFile=/var/run/mongodb/$service_name.pid
LimitFSIZE=infinity
LimitCPU=infinity
LimitAS=infinity
LimitNOFILE=64000
LimitNPROC=64000
LimitMEMLOCK=infinity
TasksMax=infinity
TasksAccounting=false

[Install]
WantedBy=multi-user.target
EOL

			echo "Starting Service..."
			systemctl daemon-reload
			systemctl start $service_name

			sleep 10
		fi
		;;
	remove)
		if [ -z "$install_dir" ] || [ -z "$service_name" ]; then
			echo "sudo bash ./mongo_setup.sh remove --install_dir install_dir --service_name service_name"
			echo "install_dir=$install_dir"
			echo "service_name=$service_name"
		else 
			if (systemctl -q is-active $service_name);then
				echo "Stopping Service..."
				systemctl stop $service_name
			fi

			if [ -f /lib/systemd/system/$service_name.service ]; then
				echo "Removing service..."
				systemctl disable $service_name
				rm -f /lib/systemd/system/$service_name.service
			fi

			if [ -f $install_dir ]; then
				echo "Removing MongoDB..."
				rm -rf $install_dir
			fi

			if id $service_name >/dev/null 2>&1; then
				echo "Removing User..."
				userdel $service_name
			fi
		fi
		;;
	*)
		echo "sudo bash ./mongo_setup.sh install --install_dir install_dir --data_dir data_dir --log_dir log_dir --service_name service_name --port port"
		echo "sudo bash ./mongo_setup.sh install --install_dir /opt/TestFEWSArchiveMongoDB --data_dir /var/lib/TestFEWSArchiveMongoDB --log_dir /var/log/TestFEWSArchiveMongoDB --service_name TestFEWSArchiveMongoDB --port 27019"
		
		echo "sudo bash ./mongo_setup.sh remove --install_dir install_dir --service_name service_name"
		echo "sudo bash ./mongo_setup.sh remove --install_dir /opt/TestFEWSArchiveMongoDB --service_name TestFEWSArchiveMongoDB"
		;;
esac
echo [$params]
echo "Complete!"
