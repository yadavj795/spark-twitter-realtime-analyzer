#!/bin/bash

HOME="/home/ec2-user"
SPARK_CONF_DIR="/usr/lib/spark/conf"
HUSTLER_BASH_LIB_DIR="/usr/lib/hustler/bin"
S3CFG_PATH="/usr/lib/hustler/s3cfg"

source $HUSTLER_BASH_LIB_DIR/qubole-bash-lib.sh

	
export consumer_key="xxxxx"
export consumer_secret="xxxxx"
export access_token="xxxxx"
export access_token_secret="xxxxx"

# Check if we are running on master
is_master=$(nodeinfo is_master)

download_kafka() {
	sudo -u ec2-user -s <<EOF
	cd /home/ec2-user
	rm -rf kafka_2.11-2.1.1.tgz kafka_2.11-2.1.1 kafka
	wget http://www.us.apache.org/dist/kafka/2.1.1/kafka_2.11-2.1.1.tgz -O $HOME/kafka_2.11-2.1.1.tgz
	mkdir -p kafka
	tar zxf kafka_2.11-2.1.1.tgz -C kafka --strip-components 1
EOF

}


install_python_packages()
{
  	echo "*** Starting package installs ***"
  	pip install kafka-python
	pip install python-twitter
	pip install tweepy
	yum install git -y
  	echo "*** Package install finished ***"
}

install_deps() {

	node_ip=$(nodeinfo node_ip)
	package_management_enabled=$(nodeinfo tapp_ui_enable_package_management)
	use_spark=$(nodeinfo use_spark)
	env_id=$(nodeinfo quboled_env_id)
	if [[ ${package_management_enabled} = "true" && ${use_spark} = "1" && z != "${env_id}" ]]; then
   		python /usr/lib/quboled/hustler/status_check.py --hostname ${node_ip}
   		if [ $? -eq 0 ]; then
       		envprefix=$(ls -d /usr/lib/envs/*| grep env-$env_id-ver- | grep "\-r-3")
       		source $envprefix/bin/activate $envprefix
       		# Python package installs
       		install_python_packages
   		else
     		echo "*** Error - Issue with node status, skipping package installation ***"
     		# Very unlikely scenario, but can take appropriate action. The extreme option could be "shutdown -h" for the VM to gracefully exit
   	fi
	else
   		echo "*** Warning - Environment not attached to the cluster ***"
   		#install into default python
	   	install_python_packages
	fi
}


configure_start_kafka() {

	mkdir -p /media/ephemeral0/kafka
	chown -R ec2-user:ec2-user /media/ephemeral0/kafka
	mkdir -p /media/ephemeral0/zookeeper
	chown -R ec2-user:ec2-user /media/ephemeral0/zookeeper

	sudo -u ec2-user -s <<EOF
		cd ~/kafka
		sed -i 's|log.dirs=/tmp/kafka-logs|log.dirs=/media/ephemeral0/kafka|g' config/server.properties
		sed -i 's|dataDir=/tmp/zookeeper|dataDir=/media/ephemeral0/zookeeper|g' config/zookeeper.properties
		bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
		sleep 10
		bin/kafka-server-start.sh -daemon config/server.properties
		sleep 20
EOF

	su - ec2-user -c "cd ~/kafka; bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic tweets"
	su - ec2-user -c  "cd ~/kafka; bin/kafka-topics.sh --describe --zookeeper localhost:2181; echo"
}


run_twitter_demo() {

	cd /tmp/
	rm -rf spark-twitter-realtime-analyzer
	hadoop fs -rmr /tmp/spark-twitter-realtime-analyzer
	git clone https://github.com/yadavj795/spark-twitter-realtime-analyzer.git
	hadoop fs -put spark-twitter-realtime-analyzer /tmp/

	#get kafka broker private ip.

	export kafka_broker=`nodeinfo master_ip`':9092'

	#set checkpoint dir and delta table location.

	export DEFLOC_LOCATION=`nodeinfo s3_default_location`

	hadoop fs -mkdir $DEFLOC_LOCATION/delta_tbl
	hadoop fs -mkdir $DEFLOC_LOCATION/delta_chk_point

	#cleaning the directories.

	hadoop fs -rmr $DEFLOC_LOCATION/delta_tbl/*
	hadoop fs -rmr $DEFLOC_LOCATION/delta_chk_point/*
  
	export table_loc="$DEFLOC_LOCATION"/delta_tbl
	export chk_point_dir="$DEFLOC_LOCATION"/delta_chk_point

	nohup /usr/lib/spark/bin/spark-submit --packages io.delta:delta-core_2.11:0.4.0  /tmp/spark-twitter-realtime-analyzer/Spark-delta-ingest.py $kafka_broker $chk_point_dir $table_loc > Spark-delta-ingest.log 2>&1 </dev/null &

	nohup /usr/lib/spark/bin/spark-submit /tmp/spark-twitter-realtime-analyzer/Spark-kafka-ingest.py $consumer_key $consumer_secret $access_token $access_token_secret > Spark-kafka-ingest.log 2>&1 </dev/null &

}

# MAIN

if [[ "$is_master" == "1" ]]; then

	install_deps
	download_kafka
	configure_start_kafka
	run_twitter_demo

fi;
