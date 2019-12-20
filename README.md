# spark-twitter-realtime-analyzer

This is a small project to demonstrate Qubole integration with Delta Lake. Here, we are using real time twitter feeds and doing some
SQL based analytics on top of that.

Pre-Requisites:

1. We have add twitter API keys in the bootstrap file and add that bootstrap file in your cluster configuration.

	consumer_key=""
	
	consumer_secret=""
	
	access_token=""
	
	access_token_secret=""
  
2. Qubole cluster must have below packages installed as part of cluster package management(PM)

  	pip install kafka-python
	
	pip install python-twitter
	
	pip install tweepy

3. We will do spark SQL analytics via Zeppeline notebook, therefore, we have to add below delta lake dependency in zeppline
   interpreter settings.
   
   	io.delta:delta-core_2.11:0.4.0
   

How to run?:

Once we have above pre-reqs completed, all he have to do is start the cluster, rest will be taken care by bootstrap script.
You can then import the notebook json file and start using those SQL queries.
