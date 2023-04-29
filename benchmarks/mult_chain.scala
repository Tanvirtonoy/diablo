import edu.uta.diablo._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random
import Math._


object Multiply extends Serializable {
  /* The size of an object */
  def sizeof ( x: AnyRef ): Long = {
    import org.apache.spark.util.SizeEstimator.estimate
    estimate(x)
  }

  def main ( args: Array[String] ) {
    val n = 4
    val N = 2

    val conf = new SparkConf().setAppName("factorization")
    spark_context = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.ERROR)

    def randomTile ( nd: Int, md: Int ): DenseMatrix = {
      val max = 10
      val rand = new Random()
      new DenseMatrix(nd,md,Array.tabulate(nd*md){ i => rand.nextDouble()*max })
    }

    def randomMatrix ( rows: Int, cols: Int ): RDD[((Int, Int),org.apache.spark.mllib.linalg.Matrix)] = {
      val l = Random.shuffle((0 until (rows+N-1)/N).toList)
      val r = Random.shuffle((0 until (cols+N-1)/N).toList)
      spark_context.parallelize(for { i <- l; j <- r } yield (i,j),number_of_partitions)
                   .map{ case (i,j) => ((i,j),randomTile(if ((i+1)*N > rows) rows%N else N,
                                                         if ((j+1)*N > cols) cols%N else N)) }
    }

    val X = randomMatrix(n,n).cache()
    val Y = randomMatrix(n,n).cache()
    val Rx = Array(1,2,3,1)
    val Ry = Array(3,1,2,1)
    val R_x = X.map{case ((i,j),v) => ((i,j),("X",i,j,Rx(i*N+j)))}
    val R_y = Y.map{case ((i,j),v) => ((i,j),("Y",i,j,Ry(i*N+j)))}
    val X_t = X.map{case (i,v) => i}.collect()
    val Y_t = Y.map{case (i,v) => i}.collect()

    val join_groups = R_x.map{case ((i,j),v) => (j,(i,v))}
                        .join(R_y.map{case ((j,k),v) => (j,(k,v))})
                        .map{case (j,((i,x),(k,y))) => ((i,k),(x,y))}

    val agg_groups = join_groups.groupByKey().collect()
    val join_group_list = join_groups.collect()
    
    var Lts_X: List[List[Int]] = List()
    for(r <- Rx) {
        var l:List[Int] = List()
        for(s <- 1 to 3) {
            if(s == r) l = 1::l
            else l = 0::l
        }
        Lts_X = l::Lts_X
    }
    var Lts_Y: List[List[Int]] = List()
    for(r <- Ry) {
        var l:List[Int] = List()
        for(s <- 1 to 3) {
            if(s == r) l = 1::l
            else l = 0::l
        }
        Lts_Y = l::Lts_Y
    }

    var Jtj_X: List[List[Int]] = List()
    for(xt <- X_t) {
        var l:List[Int] = List()
        for((i,(x,y)) <- join_group_list) {
            if(x._2 == xt._1 && x._3 == xt._2) l = 1::l
            else l = 0::l
        }
        Jtj_X = l::Jtj_X
    }
    var Jtj_Y: List[List[Int]] = List()
    for(yt <- Y_t) {
        var l:List[Int] = List()
        for((i,(x,y)) <- join_group_list) {
            if(y._2 == yt._1 && y._3 == yt._2) l = 1::l
            else l = 0::l
        }
        Jtj_Y = l::Jtj_Y
    }

    var Gjg: List[List[Int]] = List()
    for((i,jg) <- join_group_list) {
        var l:List[Int] = List()
        for((g,groups) <- agg_groups) {
            if(i._1 == g._1 && i._2 == g._2) l = 1::l
            else l = 0::l
        }
        Gjg = l::Gjg
    }

    val tp = 10
    val ts = 3
    val tf = 100
    val tg = 300

    var all_Xjs:List[List[(Int,Int)]] = List()
    var all_Ygs:List[List[(Int,Int)]] = List()

    def assign_J(j: Int, jc: Int, sc: Int, js:List[(Int,Int)]): Unit = {
        if(j >= jc) {
            all_Xjs = js::all_Xjs
            return
        }
        for(i <- 1 to sc) assign_J(j+1,jc,sc,(j,i)::js)
    }

    def assign_G(g: Int, gc: Int, sc: Int, gs:List[(Int,Int)]): Unit = {
        if(g >= gc) {
            all_Ygs = gs::all_Ygs
            return
        }
        for(i <- 1 to sc) assign_G(g+1,gc,sc,(g,i)::gs)
    }

    assign_J(0,8,3,List())
    assign_G(0,4,3,List())
    //println(all_Xjs.size) // sc^jc
    //println(all_Ygs.size) // sc^gc

    var minCost = Int.MaxValue
    var minJId = 0
    var minGId = 0
    //for(i <- 0 to all_Xjs.size-1) {
    for(i <- 0 to 4) {
        var Xjs: Array[Array[Int]] = Array.ofDim[Int](join_group_list.size, 3)
        for((ji,si) <- all_Xjs(i)) {
            Xjs(ji)(si-1) = 1
        }
        //for(j <- 0 to all_Ygs.size-1) {
        for(j <- 0 to 2) {
            var maxTp = 0
            for(s <- 1 to 3) {
                var tmp = 0
                for((ji,si) <- all_Xjs(i)) {
                    if(si == s) tmp += tp
                }
                maxTp = max(maxTp, tmp)
            }
            var maxTs = 0
            for(s <- 1 to 3) {
                var tmp = 0
                for((gi,si) <- all_Ygs(j)) {
                    if(si == s) tmp += ts
                }
                maxTs = max(maxTs, tmp)
            }
            var maxTf = 0
            for(s <- 1 to 3) {
                var tmp = 0
                for(t <- 0 to X_t.size-1) {
                    var flag = 0
                    if(Lts_X(t)(s-1) == 0) {
                        for(jii <- 0 to join_group_list.size-1) {
                            /*for((ji,si) <- all_Xjs(i)) {
                                if(ji == jii && si == s) {
                                    if(Jtj_X(t)(jii) == 1) flag = tf
                                }
                            }*/
                            if(Xjs(jii)(s-1) * Jtj_X(t)(jii) == 1) flag = tf
                        }
                    }
                    tmp += flag
                }
                for(t <- 0 to Y_t.size-1) {
                    var flag = 0
                    if(Lts_Y(t)(s-1) == 0) {
                        for(jii <- 0 to join_group_list.size-1) {
                            /*for((ji,si) <- all_Xjs(i)) {
                                if(ji == jii && si == s) {
                                    if(Jtj_Y(t)(jii) == 1) flag = tf
                                }
                            }*/
                            if(Xjs(jii)(s-1) * Jtj_Y(t)(jii) == 1) flag = tf
                        }
                    }
                    tmp += flag
                }
                maxTf = max(maxTf, tmp)
            }
            var maxTg = 0
            for(s <- 1 to 3) {
                var tmp = 0
                for((gi,si) <- all_Ygs(j)) {
                    if(si == s) {
                        for(ji <- 0 to join_group_list.size-1) {
                            if(Gjg(ji)(gi) == 1 && Xjs(ji)(s-1) == 0) tmp += tg
                        }
                    }
                }
                maxTg = max(maxTg, tmp)
            }
            println(maxTp+" "+maxTs+" "+maxTf+" "+maxTg)
            val cost = maxTf+maxTp+maxTg+maxTs
            if(cost < minCost) {
                minCost = cost
                minJId = i
                minGId = j
            }
            println(cost)
            println("Join group: ")
            all_Xjs(i).map{case(ji,si) => (join_group_list(ji),si)}.map(println)
            println("Aggregation group: ")
            all_Ygs(j).map{case(gi,si) => (agg_groups(gi),si)}.map(println)
        }
    }
    println(minCost)
    println("Join group assignment: ")
    all_Xjs(minJId).map{case(ji,si) => (join_group_list(ji),si)}.map(println)
    println("Aggregation group assignment: ")
    all_Ygs(minGId).map{case(gi,si) => (agg_groups(gi),si)}.map(println)

    spark_context.stop()
  }
}
