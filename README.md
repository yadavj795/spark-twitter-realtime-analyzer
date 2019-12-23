## Project Title

#spark-twitter-realtime-analyzer

## Getting Started

This is a small project to demonstrate Qubole integration with Delta Lake. Here, we are using real time twitter feeds and doing some
SQL based analytics on top of that.

### Prerequisites

1. We need to add twitter API keys in the bootstrap file and then add that updated bootstrap file in cluster configuration.

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

Once we have above pre-reqs completed, all he have to do is start the cluster, rest will be taken care by bootstrap script.
We can then import the notebook json file in Zeppeline and start using those SQL queries.
