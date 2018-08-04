# Graph_Assignment
Assignment on graph processing using GraphX in Apache Spark

Generate a un-directional graph RDD from a given graph data.

Compute listed vertex-based similarity measures for all the pairs of nodes in label
data file. These similarity measures are computed between two nodes by utilizing
neighborhood and/or node information of both nodes.
			Common neighbors
			Jaccard coefficient
			Adamic/Adar
			Preferential Attachment


Bonus Question: Link Prediction Model
Using the measures generated from the graph and labels from the labeled data to
predict the possibility of new link formation. Please use the following steps.
1. Create a dataset by combining measures ( as features) and class labels from
the labeled data.
2. Use decision tree based algorithm in SparkML to train the prediction model.
3. Split the dataset to generate the training data and testing data.
4. Use training data to build model and testing data to evaluate the model.
5. Present the model performance metrics: Accuracy, Recall, and Precision.

Instructions:
Please follow the program submission instructions ( same as the previous
assignments)
Must use spark and GraphX for generating measures and use SparkML for bonus questions.
More explanation on above graph measures here
(https://bmcgenomics.biomedcentral.com/articles/10.1186/1471-2164-13-S3-S5)

Data
Graph data
Use this link (https://www.dropbox.com/s/ypcsynzo28fp8pt/graph_1965_1969.csv.zip?
dl=0) to download graph data and file is formatted as shown below

Label data

click here
(hhttps://www.dropbox.com/s/oehg91f7k9zy4bj/labeled_1965_1969_1970_1974.csv.zip
?dl=0) to download label data

Full solution code for eclipse with configurations:
https://drive.google.com/open?id=1T6ttlJioo_LuqQG3jOY3X6QnfDbfma3Y
