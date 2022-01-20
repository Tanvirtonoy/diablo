import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import Math._

object Test {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    spark_context = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    param(data_frames,true)

    val N = 3000

    def f ( i: Int, j: Int = 1 ): Double = (i*11)%3+j*1.1

    var t = System.currentTimeMillis()

/*
    val x = spark_context.parallelize(List(1,2,3))

    val z = q("tensor*(10)[ (i,i) | i <- x ]")

    q("tensor*(10)[ (i,v+1) | (i,v) <- z ]")
*/

    val C = q("""

      var M = tensor(N,N)[ ((i,j),if (random()>0.5) 0.0 else random()*100) | i <- 0..N-1, j <- 0..N-1 ];

      var E = tensor*(N,N)[ ((i,j),M[i,j]) | i <- 0..N-1, j <- 0..N-1 ];
      var EE = E;

      //rdd[ ((i,j),v) | ((i,j),v) <- EE ];

      //EE;

      //tensor*(N,N)[ ((i,j),(+/c)/c.length) | ((i,k),a) <- E, ((kk,j),b) <- EE, k == kk, let c = a*b, group by (i,j) ];
      //tensor*(N,N)[ ((i,j),a+b) | ((i,j),a) <- E, ((ii,jj),b) <- EE, ii == i, jj == j ];
      tensor*(N,N)[ (((i+1)%N,j),a+1) | ((i,j),a) <- E ];
    """)

    //q("tensor*(N,N)[ ((i,j),v+1) | ((i,j),v) <- C ]")

    println(C._3.queryExecution)
    C._3.count()
    println("time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }
}
