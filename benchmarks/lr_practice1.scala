import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.log4j._
import scala.util.Random

object LinearRegression extends Serializable {
    def sizeof ( x: AnyRef ): Long = {
        import org.apache.spark.util.SizeEstimator.estimate
        estimate(x)
    }

    def main ( args: Array[String] ) {
        val conf = new SparkConf().setAppName("linear_regression")
        spark_context = new SparkContext(conf)
        val spark = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._
        conf.set("spark.logConf","false")
        conf.set("spark.eventLog.enabled","false")
        LogManager.getRootLogger().setLevel(Level.WARN)

        val rand = new Random()
        def random () = rand.nextDouble()*10
        val number_of_partitions = 10
        val N = 1000
        //val x = Array(Array(9,81),Array(3,9),Array(5,25),Array(2,4),Array(10,100))
        //val y = Array(207.0,33.0,75.0,18.0,250.0)
        val n = args(0).toInt
        val m = args(1).toInt
        val numIter = 10
        var x = Array.ofDim[Double](n,m)
        for (i <- 0 to n-1; j <- 0 to m-1) {
            if(j == 0)
                x(i)(j) = 1.0
            else 
                x(i)(j) = random()
        }
        var y = Array.ofDim[Double](n)
        for (i <- 0 to n-1) {
            y(i) = 0.0
            for(j <- 0 to m-1)
                y(i) += (j+1)*x(i)(j)
        }
        var tmp_theta = Array.ofDim[Double](m)
        for (i <- 0 to m-1) {
            tmp_theta(i) = random()
        }
        val X = spark_context.parallelize(for { i <- 0 to n-1; j <- 0 to m-1 } yield (i,j),number_of_partitions)
                    .map{case (i,j) => ((i,j),x(i)(j))}.cache
        val Y = spark_context.parallelize(0 to n-1)
                    .map(i => (i,y(i))).cache
        val lrate = args(2).toDouble

        def randomTile ( nd: Int, md: Int ): DenseMatrix = {
            val max = 10
            val rand = new Random()
            new DenseMatrix(nd,md,Array.tabulate(nd*md){ i => rand.nextDouble()*max })
        }

        def randomTile1 ( nd: Int, md: Int, id: Int ): DenseMatrix = {
            val max = 10
            val rand = new Random()
            new DenseMatrix(nd,md,Array.tabulate(nd*md){ i => x(id*N+i/N)(i%N) })
        }

        def randomMatrix ( rows: Int, cols: Int ): RDD[((Int, Int),Matrix)] = {
            val l = Random.shuffle((0 until (rows+N-1)/N).toList)
            val r = Random.shuffle((0 until (cols+N-1)/N).toList)
            spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
                        .map{ case (i,j) => ((i,j),randomTile(if ((i+1)*N > rows) rows%N else N,
                                                                if ((j+1)*N > cols) cols%N else N)) }
        }

        def randomMatrix1 ( rows: Int, cols: Int ): RDD[(Int,Matrix)] = {
            val l = Random.shuffle((0 until (rows+N-1)/N).toList)
            spark_context.parallelize(for { i <- l } yield (i),number_of_partitions)
                        .map{ case i => (i,randomTile(if ((i+1)*N > rows) rows%N else N,cols)) }
        }

        def randomMatrix2 ( rows: Int ): RDD[(Int,Matrix)] = {
            val l = Random.shuffle((0 until (rows+N-1)/N).toList)
            def md(i: Int): Int = if ((i+1)*N > rows) rows%N else N
            spark_context.parallelize(for { i <- l } yield (i),number_of_partitions)
                        .map{ case i => (i,new DenseMatrix(md(i),1,Array.tabulate(md(i)){ j => y(i*N+j)})) }
        }

        def randomMatrix3 ( rows: Int ): RDD[(Int,Matrix)] = {
            val l = Random.shuffle((0 until (rows+N-1)/N).toList)
            def md(i: Int): Int = if ((i+1)*N > rows) rows%N else N
            spark_context.parallelize(for { i <- l } yield (i),number_of_partitions)
                        .map{ case i => (i,new DenseMatrix(md(i),1,Array.tabulate(md(i)){ j => tmp_theta(i*N+j)})) }
        }

        def randomVector ( len: Int ): Array[(Int,Vector)] = {
            def md(i: Int): Int = if ((i+1)*N > len) len%N else N
            Array.tabulate((len+N-1)/N){ i => (i,new DenseVector(Array.tabulate(md(i)){ j => y(i*N+j) })) }
        }

        val Xm = randomMatrix(n,m).cache()
        val Xm1 = randomMatrix1(n,m).cache()
        val Ym = randomMatrix1(n,1).cache()

        def lrLoops(): Double = {
            val t = System.currentTimeMillis()
            var theta = tmp_theta.clone
            for(itr <- 0 to numIter-1) {
                var y_hat = Array.ofDim[Double](n)
                for(i <- 0 to n-1) {
                    for(j <- 0 to m-1)
                        y_hat(i) += x(i)(j)*theta(j)
                }
                /*var err = 0.0
                for(i <- 0 to n-1)
                    err += (y_hat(i)-y(i))*(y_hat(i)-y(i))
                err = err/n
                println("Epoch: "+(itr+1)+" MSE: "+err)*/
                var d_theta = Array.ofDim[Double](m)
                for(i <- 0 to m-1) {
                    var s = 0.0
                    for(j <- 0 to n-1) {
                        s += x(j)(i)*(y_hat(j)-y(j))
                    }
                    d_theta(i) = s
                }
                for(i <- 0 to m-1) {
                    theta(i) -= (1.0/n)*lrate*d_theta(i)
                }
            }
            println("Loops: ")
            theta.take(30).map(println)
            (System.currentTimeMillis()-t)/1000.0
        }

        def transposeBlock(mat1: Matrix): Matrix = {
            val cols = mat1.numCols
            val rows = mat1.numRows
            var arr = Array.ofDim[Double](cols*rows)
            val arr1 = mat1.toArray
            for(i <- (0 to cols-1).par) {
                for(j <- 0 to rows-1) {
                    arr(i*rows+j) = arr1(j*cols+i)
                }
            }
            new DenseMatrix(cols,rows,arr)
        }

        def sumBlocks(mat1: Matrix, mat2: Matrix, f: (Double, Double) => Double): Matrix = {
            val cols = mat1.numCols
            val rows = mat1.numRows
            val cols2 = mat2.numCols
            val rows2 = mat2.numRows
            //println("Add Mat1: ("+rows+", "+cols+"), Mat2: ("+rows2+", "+cols2+")")
            var arr = Array.ofDim[Double](rows*cols)
            val arr1 = mat1.toArray
            val arr2 = mat2.toArray
            for(i <- (0 to rows-1).par) {
                for(j <- 0 to cols-1) {
                    arr(i*cols+j) = f(arr1(i*cols+j),arr2(i*cols+j))
                }
            }
            new DenseMatrix(rows,cols,arr)
        }

        def multBlocks(mat1: Matrix, mat2: Matrix): Matrix = {
            val cols1 = mat1.numCols
            val rows1 = mat1.numRows
            val cols2 = mat2.numCols
            val rows2 = mat2.numRows
            //println("Multiply Mat1: ("+rows1+", "+cols1+"), Mat2: ("+rows2+", "+cols2+")")
            var arr = Array.ofDim[Double](rows1*cols2)
            val arr1 = mat1.toArray
            val arr2 = mat2.toArray
            for(i <- (0 to rows1-1).par) {
                var j = 0
                while(j < cols2) {
                    var k = 0
                    while(k < cols1) {
                        arr(i*cols2+j) += arr1(i*cols1+k)*arr2(k*cols2+j)
                        k+=1
                    }
                    j+=1
                }
            }
            new DenseMatrix(rows1,cols2,arr)
        }

        def lrDist1(): Double = {
            val t = System.currentTimeMillis()
            var theta = randomMatrix1(m,1).cache()
            for(itr <- 0 to numIter-1) {
                theta = Xm.map{case ((i,j),a) => (j,(i,a))}
                            .join(theta)
                            .map{case (j,((i,a),b)) => (i, multBlocks(a,b))}
                            .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
                            .join(Ym).map{case (i,(a,b)) => (i,sumBlocks(a,b,(g,h)=>g-h))}
                            .join(Xm.map{case ((i,j),a) => (i,(j,transposeBlock(a)))})
                            .map{case (i,(e,(j,a))) => (j,multBlocks(a,e))}
                            .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
                            .join(theta)
                            .map{case (i,(a,b)) => (i,sumBlocks(b,a,(g,h)=>g-(1.0/n)*lrate*h))}
                theta.cache
            }
            println("\nTheta: ")
            theta.collect.map(println)
            (System.currentTimeMillis()-t)/1000.0
        }

        def processBlocks(mat1: Matrix, mat2: Matrix, mat3: Matrix): Matrix = {
            val cols1 = mat1.numCols
            val rows1 = mat1.numRows
            val cols2 = mat2.numCols
            val rows2 = mat2.numRows
            val arr1 = mat1.toArray
            val arr2 = mat2.toArray
            var y_hat = Array.ofDim[Double](rows1*cols2)
            for(i <- (0 to rows1-1).par) {
                var j = 0
                while(j < cols2) {
                    var k = 0
                    var s = 0.0
                    while(k < cols1) {
                        s += arr1(i*cols1+k)*arr2(k*cols2+j)
                        k +=1
                    }
                    y_hat(i*cols2+j) = s
                    j += 1
                }
            }
            val arr3 = mat3.toArray
            var d_theta = Array.ofDim[Double](rows2*cols2)
            for(i <- (0 to rows2-1).par) {
                var k = 0
                while(k < cols2) {
                    var s = 0.0
                    var j = 0
                    while(j < rows1) {
                        s += arr1(j*cols1+i)*(y_hat(j*cols2+k)-arr3(j*cols2+k))
                        j+=1
                    }
                    d_theta(i*cols2+k) = s
                    k+=1
                }
            }
            new DenseMatrix(rows2,cols2,d_theta)
        }

        def lrDist2(): Double = {
            val t = System.currentTimeMillis()
            var theta = randomMatrix1(m,1).cache()
            for(itr <- 0 to numIter-1) {
                theta = Xm1.map{case (i,a) => (0,(i,a))}
                            .join(theta)
                            .map{case (j,((i,a),b)) => (i, (a,b))}
                            .join(Ym).map{case (i,(a,b)) => (0,processBlocks(a._1,a._2,b))}
                            .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
                            .join(theta)
                            .map{case (i,(a,b)) => (i,sumBlocks(b,a,(g,h)=>g-(1.0/n)*lrate*h))}
                theta.cache
            }
            println("\nTheta: ")
            theta.collect.map(println)
            (System.currentTimeMillis()-t)/1000.0
        }

        def lrDist_2(): Double = {
            val tmp = randomMatrix1(m,1).cache()
            var theta = q(" tensor*(m)[(i,v) | (i,v) <- tmp]")
            val Xd = q("tensor*(n)[(i,v) | (i,v) <- Xm1]")
            val t = System.currentTimeMillis()
            for(itr <- 0 to numIter-1) {
                val d_theta = q(" tensor*(m)[(i,+/v) | (i,a) <- Xd, (ii,th) <- theta, 0==ii, (jj,b) <- Ym, i==jj, let v = a*(a*th-b), group by i]")
                val theta1 = q(" tensor*(m)[ (i,v) | (i,dth) <- d_theta, (ii,th) <- theta, let v = th-(1.0/n)*lrate*dth]")
                theta = theta1
                theta._3.cache
            }
            println("\nTheta: ")
            theta._3.collect.map(println)
            (System.currentTimeMillis()-t)/1000.0
        }

        def sumBlocks1(v1: Vector, v2: Vector, f: (Double, Double) => Double): Vector = {
            val size1 = v1.size
            val size2 = v2.size
            var vec = Array.ofDim[Double](size1)
            val arr1 = v1.toArray
            val arr2 = v2.toArray
            for(i <- (0 to size1-1).par) {
                vec(i) = f(arr1(i),arr2(i))
            }
            new DenseVector(vec)
        }

        def processBlocks1(x_b: Matrix, th_b: Vector, y_b: Vector): Vector = {
            val cols1 = x_b.numCols
            val rows1 = x_b.numRows
            val size = th_b.size
            val x_arr = x_b.toArray
            val theta = th_b.toArray
            var y_hat = Array.ofDim[Double](rows1)
            for(i <- (0 to rows1-1).par) {
                var s = 0.0
                var j = 0
                while(j < cols1) {
                    s += x_arr(i*cols1+j)*theta(j)
                    j+=1
                }
                y_hat(i) = s
            }
            val y_arr = y_b.toArray
            var d_theta = Array.ofDim[Double](size)
            for(i <- (0 to cols1-1).par) {
                var s = 0.0
                var j = 0
                while(j < rows1) {
                    s += x_arr(j*cols1+i)*(y_hat(j)-y_arr(j))
                    j+=1
                }
                d_theta(i) = s
            }
            new DenseVector(d_theta)
        }

        def lrDist3(): Double = {
            var theta = new DenseVector(tmp_theta)
            val Yn = randomVector(n)
            val t = System.currentTimeMillis()
            val Yb = spark_context.broadcast(Yn)
            for(itr <- 0 to numIter-1) {
                val theta_b = spark_context.broadcast(theta)
                val d_theta = Xm1.map{case (i,a) => processBlocks1(a,theta_b.value,Yb.value(i)._2)}
                            .reduce{case (a,b) => sumBlocks1(a,b,(g,h)=>g+h)}
                val tmp = sumBlocks1(theta_b.value,d_theta,(g,h)=>g-(1.0/n)*lrate*h)
                theta = new DenseVector(tmp.toArray)
            }
            println("\nTheta: ")
            theta.toArray.map(println)
            (System.currentTimeMillis()-t)/1000.0
        }

        def lrDist4(): Double = {
            val tmp = randomMatrix3(m)
            var theta = q(" tensor*(m)[ (i,v) | (i,v) <- tmp]")
            val Xd = q("tensor*(n)[(i,v) | (i,v) <- Xm1]")
            val Yn = randomMatrix2(n)
            val Yb = q(" tensor*(m)[ (i,v) | (i,v) <- Yn]")
            val t = System.currentTimeMillis()
            for(itr <- 0 to numIter-1) {
                val d_theta = q(" tensor*(m)[(i,+/v) | (i,a) <- Xd, (j,th) <- theta, 0==j, (ii,b) <- Yb, i==ii, let v = processBlocks1(a,th,b), group by i]")
                val theta1 = q(" tensor*(m)[ (i,v) | (i,dth) <- d_theta, (ii,th) <- theta, let v = th-(1.0/n)*lrate*dth]")
                theta = theta1
                theta._3.cache
            }
            println("\nTheta: ")
            theta._3.collect.map(println)
            (System.currentTimeMillis()-t)/1000.0
        }

        // forces df to materialize in memory and evaluate all transformations
        // (noop write format doesn't have much overhead)
        def force ( df: DataFrame ) {
            df.write.mode("overwrite").format("noop").save()
        }

        def testMLlibLR(): Double = {
            def vect ( a: Iterable[Double] ): org.apache.spark.ml.linalg.Vector = {
                val s = Array.ofDim[Double](m)
                var count = 0
                for(x <- a) {
                    s(count) = x
                    count += 1
                }
                Vectors.dense(s)
            }

            // Create dataframes from data
            X.map{case ((i,j),v) => (i,v)}.groupByKey()
                    .map{case (i,v) => (i, vect(v))}.toDF.createOrReplaceTempView("X_d")
            Y.toDF.createOrReplaceTempView("Y_d")
            // Load training data
            val training_data = spark.sql("select Y_d._2 as label, X_d._2 as features from X_d join Y_d on X_d._1=Y_d._1")
                .rdd.map{row => LabeledPoint(
                row.getAs[Double]("label"),
                row.getAs[org.apache.spark.ml.linalg.Vector]("features")
            )}.toDF.cache()
            force(training_data)

            val t = System.currentTimeMillis()
            val lr = new LinearRegression().setMaxIter(numIter).setRegParam(0.3).setElasticNetParam(0.8)

            val lrModel = lr.fit(training_data)
            //lrModel.coefficients.toArray.map(println)
            // Summarize the model over the training set and print out some metrics
            val trainingSummary = lrModel.summary
            println(s"numIterations: ${trainingSummary.totalIterations}")
            println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
            //trainingSummary.residuals.show()
            println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
            println(s"r2: ${trainingSummary.r2}")
            (System.currentTimeMillis()-t)/1000.0
        }
        
        //println("Loop LR, Time: "+lrLoops())
        //println("Linear Regression 1, Time: "+lrDist1())
        //println("Linear Regression 2, Time: "+lrDist2())
        //println("Linear Regression 3, Time: "+lrDist3())
        //println("MLlib Time: "+testMLlibLR())
        println("Linear Regression 2, Time: "+lrDist_2())
        println("Linear Regression 4, Time: "+lrDist4())
    }
}
