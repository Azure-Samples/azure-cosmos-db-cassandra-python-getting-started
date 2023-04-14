---
page_type: sample
languages:
- python
products:
- azure
description: "Azure Cosmos DB is Microsoft's globally distributed multi-model database service."
urlFragment: azure-cosmos-db-cassandra-python-getting-started
---

# Accessing Cassandra API on Azure Cosmos DB using Python
Azure Cosmos DB is Microsoft's globally distributed multi-model database service. You can quickly create and query document, table, key-value, and graph databases, all of which benefit from the global distribution and horizontal scale capabilities at the core of Azure Cosmos DB.
This quick start demonstrates how to create an Azure Cosmos DB account for the Cassandra API by using the Azure portal. You'll then build a user profile console app, output as shown in the following image, with sample data.

## Running this sample
* Before you can run this sample, you must have the following perquisites:
	* An active Azure Cassandra API account - If you don't have an account, refer to the [Create Cassandra API account](https://docs.microsoft.com/en-us/azure/cosmos-db/create-cassandra-python). 
	* [Python 2.7]
	* [Git](http://git-scm.com/).
    * [Python Driver](https://github.com/datastax/python-driver)

1. Clone this repository using `git clone git@github.com:Azure-Samples/azure-cosmos-db-cassandra-python-getting-started.git cosmosdb`.

2. Change directories to the repo using `cd cosmosdb`

3. Next, substitute the contactPoint, username, password  in `config.py` with your Cosmos DB account's values from connectionstring panel of the portal.

	```
    'username': '<FILLME>',
    'password': '<FILLME>',
    'contactPoint': '<FILLME>',
    'port':'10350'
	```
4. Run 
   ```
   pip install Cassandra-driver 
   pip install prettytable
   pip install requests
   pip install pyopenssl
   ```
   in a terminal to install required python packages
   
5. Run `python pyquickstart.py` in a terminal to execute it.

## About the code
The code included in this sample is intended to get you quickly started with a python program that connects to Azure Cosmos DB with the Cassandra API.

## More information

- [Azure Cosmos DB](https://docs.microsoft.com/azure/cosmos-db/introduction)
- [Python SDK](https://github.com/datastax/python-driver)

