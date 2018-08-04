

package com.assign.graphEdge

// Import for first graph
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
//Import for Common neighbor
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.spark.SparkContext
import ml.sparkling.graph.operators.measures.edge.AdamicAdar
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.measures.edge.{AdamicAdar, CommonNeighbours}
import ml.sparkling.graph.operators.measures.edge.CommonNeighbours
//Import for spark ML
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession
import java.lang.Long;
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.evaluation.MulticlassMetrics





object GraphPlot {


	def main(args: Array[String]){

		val jobName = "siteRequest"
		val conf = new SparkConf().setAppName(jobName).setMaster("local[*]").set("spark.executor.memory","5g")
		val sc =  SparkContext.getOrCreate(conf)
		conf.set("spark.streaming.stopGracefullyOnShutdown","true")

		//Question 1
		val RDData = sc.textFile("/home/deola/workspace/siteRequest/data/graph_1965_1969.csv").cache();

		val PreprocessedRDD = RDData.map(_.split(",")).cache();
		val Node1Vertex = PreprocessedRDD.map(line=>(line(0).drop(1).toLong,(line(0)))).distinct()
		val Node2Vertex = PreprocessedRDD.map(line=>(line(1).drop(1).toLong,(line(4)))).distinct()
		val completeVertex = Node1Vertex.union(Node2Vertex).distinct
		val EdgesRDD = PreprocessedRDD.map(line=>(Edge(line(0).drop(1).toLong,line(1).drop(1).toLong,line(2)))).distinct()

		val graph=Graph(completeVertex,EdgesRDD).persist().cache();
          	val neigh =graph.collectNeighborIds(EdgeDirection.Either)
		val broadcastVar = sc.broadcast(neigh.collect())

		//Question 2
		val RDDataLabel = sc.textFile("/home/deola/workspace/siteRequest/data/labeled_1965_1969_1970_1974.csv").cache();
		val PreprocessedRDDLabelRaw = RDDataLabel.map(_.split(",")).cache();
		val labelDataToRdd = sc.broadcast(PreprocessedRDDLabelRaw.collect())


		
		//val output3 = broadcastVar.take(2)
		//output3.foreach(println)
    
		//val output4 = PreprocessedRDDLabelRaw.take(2)
		//output4.foreach(println)

    	val r_rdd = PreprocessedRDDLabelRaw.mapPartitions(rows => {

      	val nvalues = broadcastVar.value.toMap

      	rows.map(row=>{
                val n1 = row(0).drop(1).toLong
                
                //println(n1)
                //println(n1)
                
                val n2 = row(1).drop(1).toLong

                val n1_neigh =nvalues(n1)
                val n2_neigh =nvalues(n2)
                
                
                //println(n1_neigh)
                //println(n2_neigh)

                //Number of common neigbors
                val common_neig = n1_neigh.intersect(n2_neigh).length
                //compute Preferential
                val pre_attch = n1_neigh.length*n2_neigh.length
                
                val x = n1_neigh.intersect(n2_neigh).size/n1_neigh.union(n2_neigh).size.toDouble
                //println(common_neig+","+pre_attch)
                (n1,n2,common_neig,pre_attch,x)
                //print(x)

              })


    	})
    	//.take(100).
  	//foreach(println)
    
 	//val r_rdd1 = sc.parallelize(r_rdd)
  	val r_rdd1= r_rdd.take(2000)
  	val r_rdd2= sc.parallelize(r_rdd1)
  	val parsedData = r_rdd2.map{x => 
    	val parts1 = Array(x._1, x._2, x._3, x._4)
    	val parts2 = if (x._5 > 0.05) 1 else 0

        LabeledPoint(parts2.toDouble, Vectors.dense(parts1.map(_.toDouble)))
      	}.cache
  
	val result_U = parsedData.take(2000).foreach(println)
  	val Array(trainingDataRDD, testDataRDD) = parsedData.randomSplit(Array(0.7, 0.3))
      	//println(trainingDataRDD.take(5))
      	//println(trainingDataRDD.take(5))
      	//val numIterations = 100
     	// val stepSize = 0.00000001
     	// val model = NaiveBayes.train(trainingDataRDD, numIterations )
	
	//val logisticregression = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    	//  use logistic regression to train the model
	//val model = logisticregression.fit(trainingDataRDD)  
    	//val model = NaiveBayes.train(trainingDataRDD, lambda = 1.0)
    	val numClasses = 2
    	val categoricalFeaturesInfo = Map[Int, Int]()
	val impurity = "gini"
	val maxDepth = 5
	val maxBins = 32

	val model = DecisionTree.trainClassifier(trainingDataRDD, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)
	
    	val predictions = testDataRDD.map(p => (model.predict(p.features), p.label))
     	//val bb1 = predictions.take(5)
     	//val bb1r = trainingDataRDD.take(5)
      	//val bb1y = testDataRDD.take(5)
    	//bb1.foreach(println)
    	//bb1r.foreach(println)
    	//bb1y.foreach(println)
    
   	val accuracy = 100.0 * predictions.filter(x => x._1 == x._2).count() / (testDataRDD.count())
    
    
    	println("Total Positive Prediction Correctly = " + predictions.filter(x => x._1 == x._2).count())
    	println("Total test data = " + testDataRDD.count())
    	println("Total train data = " + trainingDataRDD.count())
    	println("Accuracy = " + accuracy)
    	//val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    	//val lrmodel = lr.fit(trainingDataRDD)
    
    
    
    	//val data = MLUtils.loadLibSVMFile(sc, "/home/deola/workspace/siteRequest/data/graph_1965_1969.csv")



     	// Get evaluation metrics.
    	//val metrics = new BinaryClassificationMetrics(predictions)
    	//val precision_value = metrics.precisionByThreshold()
    	//val recall_value = metrics.recallByThreshold()
    	//val auROC = metrics.areaUnderROC()

    	//println("Area under ROC = " + auROC)
    	val metrics = new MulticlassMetrics(predictions.map(x => (x._1,x._2)))
    	val precision_1 =  metrics.precision
     	val accuracy_1 = metrics.accuracy
     	val recall_1 = metrics.recall
     
     	println("Precision = " + precision_1,"Accuracy = " + accuracy_1,"Recall = " + recall_1)

      
      
      
      val metrics2 = new BinaryClassificationMetrics(predictions)

	// Precision by threshold
	val precision = metrics2.precisionByThreshold
	precision.foreach { case (t, p) =>
  	println(s"Threshold: $t, Precision: $p")
	}

	// Recall by threshold
	val recall = metrics2.recallByThreshold
	recall.foreach { case (t, r) =>
  	println(s"Threshold: $t, Recall: $r")
} 
      
	}

}
