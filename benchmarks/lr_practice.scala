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
        val spark_context = new SparkContext(conf)
        val spark = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._
        conf.set("spark.logConf","false")
        conf.set("spark.eventLog.enabled","false")
        LogManager.getRootLogger().setLevel(Level.WARN)

        val rand = new Random()
        def random () = rand.nextDouble()*10
        val number_of_partitions = 5
        val N = 2
        //val x = Array(Array(9,81),Array(3,9),Array(5,25),Array(2,4),Array(10,100))
        //val y = Array(207.0,33.0,75.0,18.0,250.0)
        val n = args(0).toInt
        val m = 2
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
        val lrate = 0.01

        def randomTile ( nd: Int, md: Int ): DenseMatrix = {
            val max = 10
            val rand = new Random()
            new DenseMatrix(nd,md,Array.tabulate(nd*md){ i => rand.nextDouble()*max })
        }

        def randomMatrix ( rows: Int, cols: Int ): RDD[((Int, Int),Matrix)] = {
            val l = Random.shuffle((0 until (rows+N-1)/N).toList)
            val r = Random.shuffle((0 until (cols+N-1)/N).toList)
            spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
                        .map{ case (i,j) => ((i,j),randomTile(if ((i+1)*N > rows) rows%N else N,
                                                                if ((j+1)*N > cols) cols%N else N)) }
        }

        val Xm = randomMatrix(n,m).cache()
        val Ym = randomMatrix(n,1).cache()

        def lrLoops(): Double = {
            val t = System.currentTimeMillis()
            var theta = tmp_theta.clone
            for(itr <- 0 to numIter-1) {
                var y_hat = Array.ofDim[Double](n)
                for(i <- 0 to n-1) {
                    for(j <- 0 to m-1)
                        y_hat(i) += x(i)(j)*theta(j)
                }
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

        def lrDist(): Double = {
            val t = System.currentTimeMillis()
            var theta = spark_context.parallelize(0 to m-1).map(i => (i,tmp_theta(i))).cache
            for(itr <- 0 to numIter-1) {
                theta = X.map{case ((i,j),a) => (j,(i,a))}
                            .join(theta)
                            .map{case (j,((i,a),b)) => (i,a*b)}
                            .reduceByKey(_+_)
                            .join(Y)
                            .map{case (i,(a,b)) => (i,a-b)}
                            .join(X.map{case ((i,j),a) => (i,(j,a))})
                            .map{case (i,(e,(j,a))) => (j,a*e)}
                            .reduceByKey(_+_)
                            .join(theta)
                            .map{case (i,(a,b)) => (i,b-(1.0/n)*lrate*a)}
                theta.cache
            }
            println("\nDist: ")
            theta.take(30).map(println)
            (System.currentTimeMillis()-t)/1000.0
        }

        def lrDist2(): Double = {
            val t = System.currentTimeMillis()
            var theta = spark_context.parallelize(0 to m-1).map(i => (i,tmp_theta(i))).cache
            for(itr <- 0 to numIter-1) {
                theta = X.map{case ((i,j),a) => (j,(i,a))}
                            .join(theta)
                            .map{case (j,((i,a),b)) => (i,a*b)}
                            .union(Y.map{case (i,a) => (i,-a)})
                            .join(X.map{case ((i,j),a) => (i,(j,a))})
                            .map{case (i,(e,(j,a))) => (j,-(1.0/n)*lrate*a*e)}
                            .union(theta)
                            .reduceByKey(_+_)
                theta.cache
            }
            println("\nDist2: ")
            theta.collect.map(println)
            (System.currentTimeMillis()-t)/1000.0
        }

        def transposeBlock(mat1: Matrix): Matrix = {
            val cols = mat1.numCols
            val rows = mat1.numRows
            var arr = Array.ofDim[Double](cols*rows)
            val arr1 = mat1.toArray
            for(i <- 0 to cols-1; j <- 0 to rows-1) {
                arr(i*rows+j) = arr1(j*cols+i)
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
            for(i <- 0 to rows-1; j <- 0 to cols-1) {
                arr(i*cols+j) = f(arr1(i*cols+j),arr2(i*cols+j))
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
            for(i <- 0 to rows1-1) {
                for(j <- 0 to cols2-1) {
                    for(k <- 0 to cols1-1)
                        arr(i*cols2+j) += arr1(i*cols1+k)*arr2(k*cols2+j)
                }
            }
            new DenseMatrix(rows1,cols2,arr)
        }

        def lrDist3(): Double = {
            var theta = randomMatrix(m,1).cache()
            val t = System.currentTimeMillis()
            for(itr <- 0 to numIter-1) {
                theta = Xm.map{case ((i,j),a) => (j,(i,a))}
                            .join(theta.map{case ((i,j),a) => (i, (j,a))})
                            .map{case (j,((i,a),(ii,b))) => ((i,ii), multBlocks(a,b))}
                            .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
                            .join(Ym).map{case (i,(a,b)) => (i,sumBlocks(a,b,(g,h)=>g-h))}
                            .map{case ((i,j),a) => (i,(j,a))}
                            .join(Xm.map{case ((i,j),a) => (i,(j,transposeBlock(a)))})
                            .map{case (i,((j,e),(jj,a))) => ((jj,j),multBlocks(a,e))}
                            .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
                            .join(theta)
                            .map{case (i,(a,b)) => (i,sumBlocks(b,a,(g,h)=>g-(1.0/n)*lrate*h))}
                theta.cache
            }
            println("\nDist3: ")
            theta.collect.map(println)
            (System.currentTimeMillis()-t)/1000.0
        }

        def lrDist4(): Double = {
            val t = System.currentTimeMillis()
            var theta = randomMatrix(m,1).cache()
            for(itr <- 0 to numIter-1) {
                theta = Xm.map{case ((i,j),a) => (j,(i,a))}
                            .join(theta.map{case ((i,j),a) => (i, (j,a))})
                            .map{case (j,((i,a),(ii,b))) => ((i,ii), multBlocks(a,b))}
                            .union(Ym)
                            .map{case ((i,j),a) => (i,(j,a))}
                            .join(Xm.map{case ((i,j),a) => (i,(j,a))})
                            .map{case (i,((j,e),(jj,a))) => ((jj,j),multBlocks(a,e))}
                            .union(theta)
                            .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
                theta.cache
            }
            println("\nDist4: ")
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
            //parallel
            for(i <- (0 to rows1-1).par) {
                //while
                var j = 0
                while(j <= cols2-1) {
                    var s = 0.0
                    var k = 0
                    while(k <= cols1-1) {
                        s += arr1(i*cols1+k)*arr2(k*cols2+j)
                        k += 1
                    }
                    y_hat(i*cols2+j) = s
                    j += 1
                }
            }
            val arr3 = mat3.toArray
            var d_theta = Array.ofDim[Double](rows2*cols2)
            for(i <- (0 to rows2-1).par) {
                var k = 0
                while(k <= cols2-1) {
                    var s = 0.0
                    var j = 0
                    while(j <= rows1-1) {
                        s += arr1(j*cols1+i)*(y_hat(j*cols2+k)-arr3(j*cols2+k))
                        j += 1
                    }
                    d_theta(i*cols2+k) = s
                    k += 1
                }
            }
            new DenseMatrix(rows2,cols2,d_theta)
        }
        //theta = theta - (X(i)*theta - y(i))*X(i)
        //theta and Ym broadcast join -> broadcast and map
        def lrDist5(): Double = {
            var theta = randomMatrix(m,1).cache()
            val t = System.currentTimeMillis()
            for(itr <- 0 to numIter-1) {
                theta = Xm.map{case ((i,j),a) => (j,(i,a))}
                            .join(theta.map{case ((i,j),a) => (i, (j,a))})
                            .map{case (j,((i,a),(ii,b))) => ((i,ii), (a,b))}
                            .join(Ym).map{case ((i,j),(a,b)) => ((j,j),processBlocks(a._1,a._2,b))}
                            .reduceByKey{case (a,b) => sumBlocks(a,b,(g,h)=>g+h)}
                            .join(theta)
                            .map{case (i,(a,b)) => (i,sumBlocks(b,a,(g,h)=>g-(1.0/n)*lrate*h))}
                theta.cache
            }
            println("\nDist5: ")
            theta.collect.map(println)
            (System.currentTimeMillis()-t)/1000.0
        }

        def sumBlocks1(blocks: Seq[Matrix]): Matrix = {
            var arr = Array.ofDim[Double](m)
            for { x <- blocks
            i <- (0 until m).par
            } arr(i) += x(i,0)
            new DenseMatrix(m,1,arr)
        }

        def subtractBlocks(mat1: Matrix, mat2: Matrix): Matrix = {
            val cols = mat1.numCols
            val rows = mat1.numRows
            val cols2 = mat2.numCols
            val rows2 = mat2.numRows
            //println("Add Mat1: ("+rows+", "+cols+"), Mat2: ("+rows2+", "+cols2+")")
            var arr = Array.ofDim[Double](rows*cols)
            val arr1 = mat1.toArray
            val arr2 = mat2.toArray
            for(i <- 0 to rows-1; j <- 0 to cols-1) {
                arr(i*cols+j) = arr2(i*cols+j) - (1.0/n)*lrate*arr1(i*cols+j)
            }
            new DenseMatrix(rows,cols,arr)
        }

        def lrDist6(): Double = {
            var theta = randomMatrix(m,1).toDF.cache
            Xm.toDF.createOrReplaceTempView("X_d")
            Ym.toDF.createOrReplaceTempView("Y_d")
            theta.createOrReplaceTempView("theta")
            force(theta)
            spark.udf.register("processBlocks", processBlocks _)
            spark.udf.register("sumBlocks", sumBlocks _)
            spark.udf.register("sumBlocks1", sumBlocks1 _)
            spark.udf.register("subtractBlocks", subtractBlocks _)
            val t = System.currentTimeMillis()
            for(itr <- 0 to numIter-1) {
                val t1 = spark.sql("select X_d._1._1 as _1, theta._1._2 as _2, X_d._2 as _3, theta._2 as _4 from X_d join theta on X_d._1._2==theta._1._1")
                //t1.show(false)
                t1.createOrReplaceTempView("tmp1")
                val t2 = spark.sql("select tmp1._1, tmp1._2, processBlocks(tmp1._3,tmp1._4,Y_d._2) as _3 from tmp1 join Y_d on tmp1._1==Y_d._1._1 and tmp1._2==Y_d._1._2")
                //t2.show(false)
                t2.createOrReplaceTempView("tmp2")
                val t3 = spark.sql("select struct(tmp2._2 as _1, tmp2._2 as _2) as _1, sumBlocks1(collect_list(tmp2._3)) as _2 from tmp2 group by tmp2._2")
                //t3.show(false)
                t3.createOrReplaceTempView("tmp3")
                theta = spark.sql("select theta._1 as _1, subtractBlocks(tmp3._2,theta._2) as _2 from tmp3 join theta on tmp3._1._1==theta._1._1 and tmp3._1._2==theta._1._2")
                theta.createOrReplaceTempView("theta")
                force(theta)
                //theta.show(false)
            }
            println("\nDist6: ")
            theta.show(false)
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
            // Summarize the model over the training set and print out some metrics
            val trainingSummary = lrModel.summary
            println(s"numIterations: ${trainingSummary.totalIterations}")
            println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
            //trainingSummary.residuals.show()
            println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
            println(s"r2: ${trainingSummary.r2}")
            (System.currentTimeMillis()-t)/1000.0
        }

        def addTest(): Double = {
            val t = System.currentTimeMillis()
            val size = 100
            var A = spark_context.parallelize(for { i <- 0 to size; j <- 0 to size } yield (i,j),number_of_partitions)
                    .map{case (i,j) => ((i,j),(i+5)*(j+5))}
            val B = spark_context.parallelize(for { i <- 0 to size; j <- 0 to size } yield (i,j),number_of_partitions)
                    .map{case (i,j) => ((i,j),(i+1)*(j+1))}
            for (i <- 0 to 9) {
                A = A.map{case ((i,j),a) => (j,(i,a))}
                    .join(A.map{case ((i,j),a) => (i,(j,a))})
                    .map{case (i,((j,a),(k,b))) => ((j,k),a*b)}
                    .reduceByKey(_+_)
                    .join(B)
                    .map{case ((i,j),(a,b)) => ((i,j),a+b)}
                A.cache
            }
            println("Count: "+A.count())
            (System.currentTimeMillis()-t)/1000.0
        }

        def addTest2(): Double = {
            val t = System.currentTimeMillis()
            val size = 100
            var A = spark_context.parallelize(for { i <- 0 to size; j <- 0 to size } yield (i,j),number_of_partitions)
                    .map{case (i,j) => ((i,j),(i+5)*(j+5))}
            val B = spark_context.parallelize(for { i <- 0 to size; j <- 0 to size } yield (i,j),number_of_partitions)
                    .map{case (i,j) => ((i,j),(i+1)*(j+1))}
            for(i <- 0 to 9) {
                A = A.map{case ((i,j),a) => (j,(i,a))}
                    .join(A.map{case ((i,j),a) => (i,(j,a))})
                    .map{case (i,((j,a),(k,b))) => ((j,k),a*b)}
                    .union(B)
                    .reduceByKey(_+_)
                A.cache
            }
            println("Count: "+A.count())
            (System.currentTimeMillis()-t)/1000.0
        }

        //println("Time: "+lrLoops())
        //println("Time: "+lrDist())
        //println("Time: "+lrDist2())
        //println("Time: "+lrDist3())
        //println("MLlib Time: "+testMLlibLR())
        //println("Time: "+lrDist4())
        println("Time: "+lrDist5())
        println("Time: "+lrDist6())
        //println("Time: "+addTest())
        //println("Time: "+addTest2())
    }
}