import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random
import Math._


object TrackJoin extends Serializable {
  /* The size of an object */
  def sizeof ( x: AnyRef ): Long = {
    import org.apache.spark.util.SizeEstimator.estimate
    estimate(x)
  }

  def main ( args: Array[String] ) {
    val n = 3
    val N = 2
    parami(number_of_partitions,2)

    val conf = new SparkConf().setAppName("factorization")
    spark_context = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.ERROR)

    def random(): Double = {
        val max = 10
        val rand = new Random()
        rand.nextDouble()*max
    }

    def randomTile ( nd: Int, md: Int ): DenseMatrix = {
      new DenseMatrix(nd,md,Array.tabulate(nd*md){ i => random() })
    }

    def randomMatrix ( rows: Int, cols: Int ): RDD[((Int, Int),org.apache.spark.mllib.linalg.Matrix)] = {
      val l = Random.shuffle((0 until (rows+N-1)/N).toList)
      val r = Random.shuffle((0 until (cols+N-1)/N).toList)
      spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
                   .map{ case (i,j) => ((i,j),randomTile(if ((i+1)*N > rows) rows%N else N,
                                                         if ((j+1)*N > cols) cols%N else N)) }
    }

    def randomMatrix1 ( rows: Int, cols: Int ): RDD[((Int, Int),Double)] = {
      val l = Random.shuffle((0 until rows).toList)
      val r = Random.shuffle((0 until cols).toList)
      spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
                   .map{ case (i,j) => ((i,j),random()) }
    }

    def randomMatrix2 ( rows: Int, cols: Int ): RDD[((Int, Int),Double)] = {
      val l = (0 until rows).toList
      val r = (0 until cols).toList
      spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
                   .map{ case (i,j) => ((i,j),i*11.0+j) }
    }

    val R1 = randomMatrix2(n,n).cache
    val S1 = randomMatrix2(n,n).cache
    R1.map{case ((i,j),r) => (j,(i,r))}.join(S1.map{case ((j,k),s) => (j,(k,s))})
        .map{case (j,((i,r),(k,s))) => ((i,k),r*s)}
        .reduceByKey(_+_)
        .collect().map(println)
    println("")

    val R = randomMatrix2(n,n).cache
    val Sp: Partitioner = new HashPartitioner(number_of_partitions)
    val S = randomMatrix2(n,n).partitionBy(Sp).cache
    
    /*val R1 = R.map{case ((i,j),r) => (j,i)}.repartition(number_of_partitions)
    val S1 = S.map{case ((j,k),s) => (j,k)}.repartition(number_of_partitions)
    println("Join: ")
    R1.join(S1,S1.partitioner)
        .mapPartitionsWithIndex{case (id,x) => {
                x.map{case (j,(i,k)) => (id,((i,j),(j,k)))}
            }
        }.collect().map(println)*/
    /*println("R: ")
    R.mapPartitionsWithIndex{ case (i,x) => {
                x.map{case ((j,k),v) => (i,((j,k),v))}
            }
        }.collect().map(println)
    println("S: ")
    S.mapPartitionsWithIndex{ case (i,x) => {
                x.map{case ((j,k),v) => (i,((j,k),v))}
            }
        }.collect().map(println)
    println("R+S: ")
    S.zipPartitions(R,true)((iter,iter2) => iter++iter2).mapPartitionsWithIndex{ case (i,x) => {
                x.map{case ((j,k),v) => (i,((j,k),v))}
            }
        }.collect().map(println)*/
    val hs = spark_context.broadcast(
        S.mapPartitionsWithIndex{case (i,x) => x.map{case ((j,k),s) => (j,i)}}
        .groupByKey()
        .map{case (i,x) => (i,x.toSet.toList)}
        .collectAsMap())
    def func(i: Int, x: Iterator[((Int,Int),Double)]): Iterator[(Int,((Int,Int),Double))] = {
        x.map{case ((j,k),s) => (i,((j,k),s))}
    }
    //S.mapPartitionsWithIndex(func,true).collect().map(println)
    R.flatMap{case ((i,j),r) => hs.value(j).map(p => (p,((i,j),r)))}
        .partitionBy(Sp)
        /*.mapPartitionsWithIndex{ case (i,x) => x.map{case (ii,((j,k),v)) => (i,(ii,((j,k),v)))}}
        .collect().map(println)*/
        /*.groupByKey(Sp)
        .mapPartitionsWithIndex{ case (i,x) => {
                x.flatMap{case (ii,xl) => xl.map{case ((j,k),v) => (i,(ii,((j,k),v)))}}
            }
        }.collect().map(println)*/
        .zipPartitions(S.mapPartitionsWithIndex(func,true),true)((iter1,iter2) => {
            val it1 = iter1.toList
            val it2 = iter2.toList
            it1.flatMap{case (p,((i,j),r)) => it2.flatMap{ 
                case(p1,((k,l),s)) => if(j==k) List(((i,l),r*s)) else None
            }}.toIterator
        })
        /*((iter1,iter2) => {
            val it1 = iter1.toList
            val it2 = iter2.toList
            var x: List[((Int,Int),Double)] = List()
            for((p:Int,((i:Int,j:Int),r:Double)) <- it1) {
                for((p1:Int,((k:Int,l:Int),s:Double)) <- it2) {
                    if(j==k) x = ((i,l),r*s)::x
                }
            }
            x.toIterator
        })*/
        .reduceByKey(_+_)
        .collect().map(println)
    
    spark_context.stop()
  }
}
