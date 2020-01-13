## Project Title

#spark-twitter-realtime-analyzer

## Getting Started

This is a small project to demonstrate Qubole integration with Delta Lake  with full Acid, transaction support and time travel etc. on Delta lake. Here, we are using real time twitter feeds and doing some SQL based analytics on top of that.

## Demo Architecture

Twitter Python App ==> Kafka ==> Spark Structure Streaming ==> Delta Lake ===> Spark Sql Analytics

### Prerequisites

1. We need to add twitter API keys in the bootstrap file and then add updated bootstrap file in the cluster node bootstrap section.

	consumer_key=""
	
	consumer_secret=""
	
	access_token=""
	
	access_token_secret=""
  
2. Qubole cluster must have below packages installed as part of cluster package management(PM)

  	pip install kafka-python
	
	pip install python-twitter
	
	pip install tweepy

3. Will do spark SQL analytics via Zeppeline notebook, therefore, we have to add below delta lake dependency in zeppline
   interpreter settings.
   
   	io.delta:delta-core_2.11:0.4.0
   

### How to run?

Once we have above pre-reqs completed, all we have to do is to start the cluster, rest will be taken care by the bootstrap script. We can then import the notebook json file in Zeppeline and start using SQL queries.
