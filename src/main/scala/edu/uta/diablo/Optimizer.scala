/*
 * Copyright © 2020-2022 University of Texas at Arlington
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uta.diablo
import scala.annotation.tailrec
import scala.collection.mutable.Map

object Optimizer {
  import AST._
  import Normalizer.{normalizeAll,comprVars,inverse}
  import ComprehensionTranslator.{isBlockTensor,isTensor,qual_vars,correlated_indices}
  import Typechecker.{typecheck}
  import Lifting.lift

  /* general span for comprehensions; if a qualifier matches, split there and continue with cont */
  @tailrec
  def matchQ ( qs: List[Qualifier], filter: Qualifier => Boolean,
               cont: List[Qualifier] => Option[List[Qualifier]] ): Option[List[Qualifier]]
    = qs match {
        case q::r
          if filter(q)
          => cont(qs) match {
               case c@Some(_) => c
               case _ => matchQ(r,filter,cont)
             }
        case _::r
          => matchQ(r,filter,cont)
        case _ => None
      }

  def matchF ( e: Expr, filter: Expr => Boolean,
               cont: Expr => Option[List[Expr]] ): Option[List[Expr]]
    = if (filter(e))
        cont(e) match {
          case c@Some(_) => c
          case _ => accumulate[Option[List[Expr]]](e,matchF(_,filter,cont),_.orElse(_),None)
        }
      else e match {
        case flatMap(f,u)
          => matchF(f,filter,cont)
        case Block(_:+b)
          => matchF(b,filter,cont)
        case _ => accumulate[Option[List[Expr]]](e,matchF(_,filter,cont),_.orElse(_),None)
      }

  def isArray ( e: Expr ): Boolean
    = e match {
        case Lift(name,_) if isBlockTensor(name) => true
        case Lift(name,_) if isTensor(name) => true
        case _ => false
      }

  /* matches ...,i<-range(...),...,p<-e,...,v==i,... where p contains v */
  def findRangeGen ( qs: List[Qualifier] ): Option[List[Qualifier]]
    = matchQ(qs,{ case Generator(_,Range(_,_,_)) => true; case _ => false },
                { case (ig@Generator(VarPat(index),Range(_,_,_)))::r
                    => matchQ(r,{ case Generator(_,x) => isArray(x); case _ => false },
                                { case (g@Generator(TuplePat(List(p,_)),_))::s
                                    => matchQ(s,{ case Predicate(MethodCall(Var(v),"==",List(ie)))
                                                    => patvars(p).contains(v) &&
                                                       inverse(ie,index,Var(v)).nonEmpty
                                                  case _ => false },
                                              { case c::_
                                                  => Some(List(ig,g,c))
                                                case _ => None })
                                  case _ => None })
                  case _ => None })

  /* matches ...,p<-e,...,i<-range(...),...,v==i,... where p contains v */
  def findRangeGen2 ( qs: List[Qualifier] ): Option[List[Qualifier]]
    = matchQ(qs,{ case Generator(_,x) => isArray(x); case _ => false },
                { case (g@Generator(TuplePat(List(p,_)),_))::s
                    => matchQ(s,{ case Generator(_,Range(_,_,_)) => true; case _ => false },
                                { case (ig@Generator(VarPat(index),Range(_,_,_)))::r
                                    => matchQ(r,{ case Predicate(MethodCall(Var(v),"==",List(ie)))
                                                    => patvars(p).contains(v) &&
                                                       inverse(ie,index,Var(v)).nonEmpty
                                                  case _ => false },
                                              { case c::_
                                                  => Some(List(ig,g,c))
                                                case _ => None })
                                  case _ => None })
                  case _ => None })

  def findRangeGen3 ( qs: List[Qualifier] ): Option[List[Qualifier]] = {
    val qvars = qual_vars(qs)
    matchQ(qs,{ case Generator(_,Range(_,_,_)) => true; case _ => false },
                { case (g@Generator(VarPat(i),_))::s
                    => matchQ(s,{ case Generator(_,Range(_,_,_)) => true; case _ => false },
                                { case (ig@Generator(VarPat(j),_))::r
                                    => matchQ(r,{ case Predicate(MethodCall(ie,"==",List(je)))
                                                    if freevars(ie).contains(i)
                                                       && freevars(je).contains(j)
                                                       && freevars(ie,List(i)).toSet.intersect(qvars).isEmpty
                                                       && freevars(je,List(j)).toSet.intersect(qvars).isEmpty
                                                       && inverse(je,j,ie).nonEmpty
                                                    => true
                                                  case Predicate(MethodCall(je,"==",List(ie)))
                                                    if freevars(ie).contains(i)
                                                       && freevars(je).contains(j)
                                                       && freevars(ie,List(i)).toSet.intersect(qvars).isEmpty
                                                       && freevars(je,List(j)).toSet.intersect(qvars).isEmpty
                                                       && inverse(je,j,ie).nonEmpty
                                                    => true
                                                  case _ => false },
                                              { case c::_
                                                  => Some(List(g,ig,c))
                                                case _ => None })
                                  case _ => None })
                  case _ => None })
         }

  /* finds a sequence of predicates in qs that imply x=y */
  def findEqPred ( x: String, y: String, qs: List[Qualifier] ): Option[List[Qualifier]]
    = matchQ(qs,{ case Predicate(MethodCall(Var(v1),"==",List(Var(v2))))
                    => v1!=v2 && (v1==x || v1==y || v2==x || v2==y)
                  case _ => false },
                { case (p@Predicate(MethodCall(Var(v1),"==",List(Var(v2)))))::s
                    => (if ((v1==x && v2==y) || (v2==x && v1==y))
                           Some(Nil)
                        else if (v1==x && v2==y) findEqPred(v2,y,s)
                        else if (v1==y && v2==x) findEqPred(x,v2,s)
                        else if (v2==x && v1==y) findEqPred(v1,y,s)
                        else findEqPred(x,v1,s)).map(p::_)
                  case _ => None })

  /* matches ...,(i1,x1)<-e,...,(i2,x2)<-e,...,i1=i2,...
   * or      ...,((i1,j1),x1)<-e,...,((i2,j2),x2)<-e,...,i1=i2,...,j1=j2,...   */
  def findEqGen ( qs: List[Qualifier] ): Option[List[Qualifier]]
    = matchQ(qs,{ case Generator(_,x) if isArray(x) => true; case _ => false },
                { case (g1@Generator(TuplePat(List(VarPat(i1),_)),x))::r
                    => matchQ(r,{ case Generator(_,y) if x==y => true; case _ => false },
                                { case (g2@Generator(TuplePat(List(VarPat(i2),_)),y))::s
                                    => for { p <- findEqPred(i1,i2,s)
                                           } yield g1::g2::p
                                  case _ => None })
                  case (g1@Generator(TuplePat(List(TuplePat(List(VarPat(i1),VarPat(j1))),_)),x))::r
                    => matchQ(r,{ case Generator(_,y) if x==y => true; case _ => false },
                                { case (g2@Generator(TuplePat(List(TuplePat(List(VarPat(i2),VarPat(j2))),_)),x))::s
                                    => for { p1 <- findEqPred(i1,i2,s)
                                             p2 <- findEqPred(j1,j2,s)
                                           } yield g1::g2::(p1++p2)
                                  case _ => None })
                  case _ => None })

  def findBoundRange ( qs: List[Qualifier] ): Option[List[Qualifier]]
    = matchQ(qs,{ case Generator(VarPat(_),Range(_,_,_)) => true; case _ => false },
                { case (g1@Generator(VarPat(v),Range(_,_,_)))::s
                    => matchQ(s,{ case p@Predicate(MethodCall(Var(v1),"==",List(e)))
                                    => v == v1 && !freevars(Comprehension(e,s.dropWhile(_ != p))).contains(v1)
                                  case p@Predicate(MethodCall(e,"==",List(Var(v1))))
                                    => v == v1 && !freevars(Comprehension(e,s.dropWhile(_ != p))).contains(v1)
                                  case _ => false },
                                { case Predicate(MethodCall(e,"==",List(Var(v1))))::_
                                    => Some(List(g1,Predicate(MethodCall(Var(v1),"==",List(e)))))
                                  case g2::_
                                    => Some(List(g1,g2))
                                  case _ => None })
                  case _ => None })

  def findLetBound ( qs: List[Qualifier] ): Option[List[Qualifier]]
    = matchQ(qs,{ case Predicate(MethodCall(Var(v),"==",List(e))) => true; case _ => false },
                { case (c@Predicate(MethodCall(_,_,List(e))))::s
                    => matchQ(s,{ case LetBinding(p,u) => u == e; case _ => false },
                                { case lb::_ => Some(List(c,lb))
                                  case _ => None })
                  case _ => None })

  /* true if the group-by key is a constant; then there will be just one group */
  def constantKey ( key: Expr ): Boolean
    = key match {
         case Tuple(ts) => ts.forall(constantKey)
         case IntConst(_) => true
         case LongConst(_) => true
         case DoubleConst(_) => true
         case BoolConst(_) => true
         case CharConst(_) => true
         case StringConst(_) => true
         case _ => false
      }

  /* true if the group-by key is unique, then the groups are singletons */
  def uniqueKey ( key: Expr, qs: List[Qualifier] ): Boolean = {
     val is = qs.takeWhile(!_.isInstanceOf[GroupByQual]).flatMap {
                  case Generator(VarPat(i),Range(_,_,_))
                    => List(i)
                  case Generator(TuplePat(List(pi,pv)),e)
                    if isArray(e)
                    => patvars(pi)
                  case Generator(_,_)
                    => return false
                  case _ => Nil
              }
     def comps ( k: Expr ): List[String]
       = k match {
            case Tuple(ts) => ts.flatMap(comps)
            case Var(i) => List(i)
            case Project(u,_) => comps(u)
            case Nth(u,_) => comps(u)
            case MethodCall(u,op,List(c))
              if (List("+","-","*").contains(op) && constantKey(c))
              => comps(u)
            case MethodCall(c,op,List(u))
              if (List("+","-","*").contains(op) && constantKey(c))
              => comps(u)
            case _ => List("%") // will fail to match
         }
     comps(key).toSet.equals(is.toSet)
  }

  private def replace[T] ( x: T, y: T, s: List[T] )
    = s.map{ i => if (i == x) y else i }

  var QLcache: Option[List[Qualifier]] = None

  def max ( x: Expr, y: Expr ): Expr
    = (x,y) match {
        case (IntConst(n),IntConst(m))
          => IntConst(Math.max(n,m))
        case _ => MethodCall(Var("Math"),"max",List(x,y))
    }

  def min ( x: Expr, y: Expr ): Expr
    = (x,y) match {
        case (IntConst(n),IntConst(m))
          => IntConst(Math.min(n,m))
        case _ => MethodCall(Var("Math"),"min",List(x,y))
    }

  def correlatedRangeTraversals ( e: Expr ): Option[List[Expr]]
    = matchF(e,{ case flatMap(_,Range(_,_,_)) => true; case _ => false },
               { case b1@flatMap(Lambda(VarPat(i),c1),_)
                   => matchF(c1,{ case flatMap(_,Range(_,_,_)) => true; case _ => false },
                                { case b2@flatMap(Lambda(VarPat(j),c2),_)
                                    => matchF(c2,{ case c@IfE(MethodCall(e1,"==",List(e2)),b,Seq(Nil))
                                                     => (e1 == Var(i) && e2 == Var(j)) ||
                                                        (e2 == Var(i) && e1 == Var(j))
                                                   case _ => false },
                                                 c => Some(List(b1,b2,c)))
                                  case _ => None })
                 case _ => None })
  
  /*def dimension( e: Expr): List[Int] = 
    e match {
      /*case Call("rdd_block_tensor_2_0",List(d,s,comp))
        => d match {
          case Tuple(value) => 
            val vv = value.map{ 
              case IntConst(y) => y
              case _ => 0
            }
            vv
          case _ => List(0)
        }*/
      case Var(x)
        => val x_type = typecheck(e)
          println(x_type)
          x_type match {
            case StorageType(_,_,dims) => dims.map{
              case IntConst(value) => value
              case e1@Nth(v,idx) => println(typecheck(e1));10 // TODO: fix the Nth expr
              case _ => 0
            }
            case _ => List(0)
          }
      case Comprehension(i,qs) 
        => 
      case _ => List(0)
    }

  def cost( g: Generator): Int = 
    g match {
      case Generator(ii,Call(_,List(_,_,c@Comprehension(_,_))))
        => val e_vars = patvars(ii).toSet
          e_vars.map(println)
          cost(c)
      case Generator(ii,x)
        => val e_vars = patvars(ii).toSet
          e_vars.map(println)
          cost(x)
    }
  def cost( e: Expr): Int = 
  { println(e)
    e match {
      /*case Call("rdd_block_tensor_2_0",List(d,s,comp))
        => dimension(e).reduce(_+_)*/
      case Var(t)
        => val e1 = lift(e)
          dimension(e).reduce(_+_)
      case Comprehension(i,qs)
        => val generator_cost = qs.map{ 
          // TODO: Create a hashmap index -> Int dimensions
          case gen@Generator(ii,x)
            => val e_vars = patvars(ii).toSet
            //e_vars.map(println)
            println(cost(gen))
            // TODO: Modify hashmap index -> Int dimensions
            1
          case _ => 1
        }
        val predicate_cost = qs.map{
          case p@Predicate(u)
            => val e_vars = freevars(u).toSet
            e_vars.map(println)
            1
          case _ => 1
        }
        0
        /*val e_vars = freevars(e).toSet
        val cis = correlated_indices(List(qs1,qs2,p),e_vars)
        cost(qs1)*cost(qs2)/cis.size*/
      case _ => 0
    }
  }*/

  def dimension( e: Expr): List[Int] = 
    e match {
      case Var(x)
        => val x_type = typecheck(e)
          x_type match {
            case StorageType(_,_,dims) => dims.map{
              case IntConst(value) => value
              case e1@Nth(v,idx) => 10 // TODO: fix the Nth expr
              case _ => 0
            }
            case _ => List(0)
          }
      case Tuple(dims)
        => dims.map{
          case IntConst(value) => value
          case _ => 0
        }
      case _ => List(0)
    }

  def cost( e: Expr): (Int,List[Int]) =
    e match {
      case Var(t)
        => val dims = dimension(e)
          (0,dims) // Cost is 0 for a tensor
      case Comprehension(Tuple(List(h,r)),qs)
        => var dimension_map: Map[String,Int] = Map() // Map to store size of each index
          val generator_cost = qs.map{
          case Generator(ii,Call(_,List(dn,_,c@Comprehension(_,List(Generator(_,Range(_,_,_)),Generator(_,Range(_,_,_)))))))
            => val dims = dimension(dn)
            val e_vars = patvars(ii).toSet.toList
            for(i<- 0 to dims.length-1)
              dimension_map = dimension_map + (e_vars(i)->dims(i))
            (0,dims)
          case Generator(ii,Call(_,List(_,_,c@Comprehension(_,_))))
            => val e_vars = patvars(ii).toSet.toList
              val comp_cost = cost(c)
              for(i<- 0 to comp_cost._2.length-1)
                dimension_map = dimension_map + (e_vars(i)->comp_cost._2(i))
              comp_cost
          case Generator(ii,x)
            => val e_vars = patvars(ii).toSet.toList
              lift(x)
              val comp_cost = cost(x)
              for(i<- 0 to comp_cost._2.length-1)
                dimension_map = dimension_map + (e_vars(i)->comp_cost._2(i))
              comp_cost
          case p@Predicate(u)
            => val pred_vars = freevars(u).toSet.toList
            dimension_map(pred_vars(0)) = 0
            (0,List())
          case _ => (0,List())
        }
        var comprehension_cost = 1
        for(dm <- dimension_map)
          if(dm._2 != 0) 
            comprehension_cost = comprehension_cost*dm._2
        var dim_list: List[Int] = List()
        val head_indices = freevars(h)
        for(hi <- head_indices)
          dim_list = dim_list:+dimension_map(hi)
        for(gc <- generator_cost) {
          comprehension_cost+=gc._1
        }
        (comprehension_cost,dim_list)
      case _ => (0,List(0))
    }
  
  def isNestedComprehension( g: Generator): Boolean =
    g match {
      case Generator(ii,Call(_,List(_,_,c@Comprehension(_,_)))) => true
      case _ => false
    }

  var CRTcache: Option[List[Expr]] = None

  def optimize ( e: Expr ): Expr =
    e match {
      case Comprehension(h,qs)
        if { QLcache = findRangeGen(qs); QLcache.nonEmpty }
        => // eliminate a range generator
           QLcache match {
             case Some(List( ig@Generator(VarPat(i),ir@Range(n1,n2,n3)),
                             g@Generator(p,u),
                             c@Predicate(MethodCall(Var(v),"==",List(ie))) ))
                => val m1 = subst(i,n1,ie)
                   val m2 = subst(i,n2,ie)
                   val m13 = subst(i,MethodCall(n1,"+",List(n3)),ie)
                   val m3 = if (n3 == IntConst(1)) n3
                              else MethodCall(MethodCall(m13,"-",List(m1)),"/",List(n3))
                   val gs = List(Generator(p,u),
                                 LetBinding(VarPat(i),inverse(ie,i,Var(v)).get))++
                              (if (ir==u) Nil else List(Predicate(Call("inRange",List(Var(v),m1,m2,m3)))))
                   val nqs = qs.diff(List(g,c)).flatMap( x => if (x==ig) gs else List(x))
                   optimize(Comprehension(h,nqs))
             case _ => apply(e,optimize)
           }
      case Comprehension(h,qs)
        if { QLcache = findRangeGen2(qs); QLcache.nonEmpty }
        => // eliminate a range generator (first the array generator, then the range generator)
           QLcache match {
             case Some(List( ig@Generator(VarPat(i),ir@Range(n1,n2,n3)),
                             g@Generator(p,u),
                             c@Predicate(MethodCall(Var(v),"==",List(ie))) ))
                => val m1 = subst(i,n1,ie)
                   val m2 = subst(i,n2,ie)
                   val m13 = subst(i,MethodCall(n1,"+",List(n3)),ie)
                   val m3 = if (n3 == IntConst(1)) n3
                              else MethodCall(MethodCall(m13,"-",List(m1)),"/",List(n3))
                   val gs = List(Generator(p,u),
                                 LetBinding(VarPat(i),inverse(ie,i,Var(v)).get))++
                              (if (ir==u) Nil else List(Predicate(Call("inRange",List(Var(v),m1,m2,m3)))))
                   val nqs = qs.diff(List(ig,c)).flatMap( x => if (x==g) gs else List(x))
                   optimize(Comprehension(h,nqs))
             case _ => apply(e,optimize)
           }
      case Comprehension(h,qs)
        if { QLcache = findRangeGen3(qs); QLcache.nonEmpty }
        => // if two range generators are correlated, eliminate the second range generator 
           QLcache match {
             case Some(List( ig@Generator(_,_),
                             jg@Generator(VarPat(j),Range(n1,n2,n3)),
                             c@Predicate(MethodCall(ie,"==",List(je))) ))
                => val (ip,jp) = if (freevars(je).contains(j)) (ie,je) else (je,ie)
                   val bs = List(LetBinding(VarPat(j),inverse(jp,j,ip).get),
                                 Predicate(Call("inRange",List(Var(j),n1,n2,n3))))
                   val nqs = qs.diff(List(c)).flatMap( x => if (x==jg) bs else List(x))
                   optimize(Comprehension(h,nqs))
             case _ => apply(e,optimize)
           }
      case Comprehension(h,qs)
        if { QLcache = findEqGen(qs); QLcache.nonEmpty }
        => // eliminate duplicate generators over arrays that have equal index value
           QLcache match {
             case Some( (g1@Generator(p1,_))::(g2@Generator(p2,_))::c )
               => val nqs = replace(g2,LetBinding(p2,toExpr(p1)),qs)
                  optimize(Comprehension(h,nqs))
             case _ => apply(e,optimize)
           }

      case Comprehension(h,qs)
        if { QLcache = findBoundRange(qs); QLcache.nonEmpty }
        => // eliminate bound range generators
           QLcache match {
             case Some(List(g@Generator(p,_),c@Predicate(MethodCall(Var(v1),"==",List(ev)))))
               => val nqs = replace(c,Predicate(BoolConst(true)),replace(g,LetBinding(p,ev),qs))
                  optimize(Comprehension(h,nqs))
             case _ => apply(e,optimize)
           }
      case Comprehension(h,qs)
        if { QLcache = findLetBound(qs); QLcache.nonEmpty }
        => // simplify let-binding using an equality condition
           QLcache match {
             case Some(List(Predicate(MethodCall(Var(v),"==",List(e))),
                            lb@LetBinding(p,w)))
               => val nqs = replace(lb,LetBinding(p,Var(v)),qs)
                  optimize(Comprehension(h,nqs))
             case _ => apply(e,optimize)
           }
      /*case Comprehension(
            Tuple(List(i,reduce(op1,v))),
            List(
              Generator(h,
                Call(rdd_block_tensor_2_0,
                  List(l1,l2,
                    Comprehension(Tuple(List(j,
                      reduce(op2,v1))),
                      List(
                      qs1,
                      Generator(TuplePat(List(TuplePat(List(jj_1,jj_2)),bb)),y),
                      p1,LetBinding(v2,MethodCall(a1,op3,List(b1))),qs2)
                    )
                  )
                )
              ),
              Generator(TuplePat(List(TuplePat(List(kk_1,kk_2)),cc)),z),
              p2,LetBinding(v3,MethodCall(a2,op4,List(c1))),qs3
            )
          )
        => Comprehension(
            Tuple(List(i,reduce(op1,v))),
            List(
              qs1,
              Generator(TuplePat(List(TuplePat(List(jj_1,kk_2)),VarPat("ab"))),
                Call(rdd_block_tensor_2_0,
                  List(l1,l2,
                    Comprehension(Tuple(List(Tuple(List(Var("jj_1"),Var("kk_2"))),
                      reduce(op2,v1))),
                      List(
                      Generator(TuplePat(List(TuplePat(List(jj_1,jj_2)),bb)),y),
                      Generator(TuplePat(List(TuplePat(List(kk_1,kk_2)),cc)),z),
                      p2,LetBinding(v2,MethodCall(b1,op4,List(c1))),
                      GroupByQual(
                        TuplePat(List(jj_1,kk_2)),Tuple(List(Var("jj_1"),Var("kk_2")))
                      ))
                    )
                  )
                )
              ),
              p1,LetBinding(v3,MethodCall(a1,op3,List(a2))),qs3
            )
          )*/

      /*case Comprehension(
            Tuple(List(i,reduce(op1,v))),
            List(
              Generator(h,
                Call(f,
                  List(l1,l2,
                    Comprehension(Tuple(List(j,
                      reduce(op2,v1))),
                      List(
                      qs1@Generator(_,_),
                      Generator(TuplePat(List(TuplePat(List(jj_1,jj_2)),bb)),y),
                      p1@Predicate(_),LetBinding(v2,MethodCall(a1,op3,List(b1))),qs2@GroupByQual(_,_))
                    )
                  )
                )
              ),
              Generator(TuplePat(List(TuplePat(List(kk_1,kk_2)),cc)),z),
              p2@Predicate(_),LetBinding(v3,MethodCall(a2,op4,List(c1))),qs3@GroupByQual(_,_)
            )
          )
        => println(cost(e))
          val e1 = Comprehension(
            Tuple(List(i,reduce(op1,v))),
            List(
              qs1,
              Generator(TuplePat(List(TuplePat(List(jj_1,kk_2)),VarPat("ab"))),
                Call(f,
                  List(l1,l2,
                    Comprehension(Tuple(List(Tuple(List(Var("jj_1"),Var("kk_2"))),
                      reduce(op2,v1))),
                      List(
                      Generator(TuplePat(List(TuplePat(List(jj_1,jj_2)),bb)),y),
                      Generator(TuplePat(List(TuplePat(List(kk_1,kk_2)),cc)),z),
                      p2,LetBinding(v2,MethodCall(b1,op4,List(c1))),
                      GroupByQual(
                        TuplePat(List(jj_1,kk_2)),Tuple(List(Var("jj_1"),Var("kk_2")))
                      ))
                    )
                  )
                )
              ),
              p1,LetBinding(v3,MethodCall(a1,op3,List(a2))),qs3
            )
          )
          println(cost(e1))
          e1*/
      case Comprehension(
            Tuple(List(Tuple(List(iii_1,kkk_2)),reduce(op1,v))),
            List(
              Generator(TuplePat(List(i,ab)),
                Call(f,
                  List(l1,l2,
                    Comprehension(Tuple(List(j,
                      reduce(op2,v1))),
                      List(
                      qs1@Generator(_,_),
                      Generator(TuplePat(List(TuplePat(List(jj_1,jj_2)),bb)),y),
                      p1@Predicate(MethodCall(iii_2,"==",List(jjj_1))),LetBinding(v2,MethodCall(a1,op3,List(b1))),qs2@GroupByQual(_,_))
                    )
                  )
                )
              ),
              Generator(TuplePat(List(TuplePat(List(kk_1,kk_2)),cc)),z),
              p2@Predicate(_),LetBinding(v3,MethodCall(a2,op4,List(c1))),qs3@GroupByQual(_,_)
            )
          )
        => val cost_e = cost(e)
          val e1 = Comprehension(
            Tuple(List(Tuple(List(iii_1,kkk_2)),reduce(op1,v))),
            List(
              qs1,
              Generator(TuplePat(List(TuplePat(List(jj_1,kk_2)),ab)),
                Call(f,
                  List(l1,l2,
                    Comprehension(Tuple(List(Tuple(List(jjj_1,kkk_2)),
                      reduce(op2,v1))),
                      List(
                      Generator(TuplePat(List(TuplePat(List(jj_1,jj_2)),bb)),y),
                      Generator(TuplePat(List(TuplePat(List(kk_1,kk_2)),cc)),z),
                      p2,LetBinding(v2,MethodCall(b1,op4,List(c1))),
                      GroupByQual(
                        TuplePat(List(jj_1,kk_2)),Tuple(List(jjj_1,kkk_2))
                      ))
                    )
                  )
                )
              ),
              p1,LetBinding(v3,MethodCall(a1,op3,List(a2))),qs3
            )
          )
          val cost_e1 = cost(e1)
          if(cost_e._1 > cost_e1._1) e1
          else e

      /*case Comprehension(h,g1@Generator(_,_)::g2@Generator(_,_)::_) => 
        if(isNestedComprehension(g1) || isNestedComprehension(g2))
        => val cost1 = cost(e)
          if(isNestedComprehension(g1)) {
            e match {
              case Comprehension(hi,
                List(
                  Generator(TuplePat(List(i,ab)),
                    Call(f,
                      List(l1,l2,
                        Comprehension(Tuple(List(j,
                          reduce(op2,v1))),
                          List(
                          qs1@Generator(_,_),
                          Generator(TuplePat(List(TuplePat(List(jj_1,jj_2)),bb)),y),
                          p1@Predicate(_),LetBinding(v2,MethodCall(a1,op3,List(b1))),qs2@GroupByQual(_,_))
                        )
                      )
                    )
                  ),
                  Generator(TuplePat(List(TuplePat(List(kk_1,kk_2)),cc)),z),
                  p2@Predicate(_),LetBinding(v3,MethodCall(a2,op4,List(c1))),qs3@GroupByQual(_,_)
                )
              ) => val e1 = Comprehension(
                  Tuple(List(i,reduce(op1,v))),
                  List(
                    qs1,
                    Generator(TuplePat(List(TuplePat(List(jj_1,kk_2)),ab)),
                      Call(f,
                        List(l1,l2,
                          Comprehension(Tuple(List(Tuple(List(Var("jj_1"),Var("kk_2"))),
                            reduce(op2,v1))),
                            List(
                            Generator(TuplePat(List(TuplePat(List(jj_1,jj_2)),bb)),y),
                            Generator(TuplePat(List(TuplePat(List(kk_1,kk_2)),cc)),z),
                            p2,LetBinding(v2,MethodCall(b1,op4,List(c1))),
                            GroupByQual(
                              TuplePat(List(jj_1,kk_2)),Tuple(List(Var("jj_1"),Var("kk_2")))
                            ))
                          )
                        )
                      )
                    ),
                    p1,LetBinding(v3,MethodCall(a1,op3,List(a2))),qs3
                  )
                )
                val cost2 = cost(e1)
            }
          }
          else if(isNestedComprehension(g2)) {

          }*/
      
      case Comprehension(h,qs)
        => qs.span{ case GroupByQual(p,k) if constantKey(k) => false; case _ => true } match {
              case (r,GroupByQual(VarPat(k),u)::s)
                => // a group-by on a constant key can be eliminated
                   val vs = comprVars(r).map(v => LetBinding(VarPat(v),Comprehension(Var(v),r)))
                   val bs = LetBinding(VarPat(k),u)::vs
                   Comprehension(h,bs++s)
              case _
                => qs.span{ case GroupByQual(p,k) if uniqueKey(k,qs) => false; case _ => true } match {
                      case (r,GroupByQual(VarPat(k),u)::s)
                        => // a group-by on a unique key can be eliminated after lifting each var v to {v}
                           val vs = comprVars(r).map(v => LetBinding(VarPat(v),Seq(List(Var(v)))))
                           val bs = LetBinding(VarPat(k),u)+:vs
                           Comprehension(h,r++bs++s)
                      case _
                        => // a group-by on a unique key
                           qs.span{ case Generator(TuplePat(List(k,v)),u)
                                      if ComprehensionTranslator.isTiled(u)
                                      => false
                                    case _ => true } match {
                             case (r,(x@Generator(TuplePat(List(k,v)),u))::(s:+GroupByQual(p,gk)))
                               if toExpr(k) == gk
                               => val groupByVars = patvars(p)
                                  val liftedVars = freevars(Comprehension(h,Nil),groupByVars)
                                                      .intersect(comprVars((r:+x)++s))
                                  val lp = liftedVars match {
                                              case List(v)
                                                => VarPat(v)
                                              case _
                                                => TuplePat(liftedVars.map(VarPat))
                                           }
                                  val bs = List(LetBinding(lp,Comprehension(toExpr(lp),s)),
                                                LetBinding(p,gk))
                                  normalizeAll(Comprehension(h,(r:+x)++bs))
                             case _ => apply(e,optimize)
                           }
                   }
           }
      case _  // nested correlated flatMaps over ranges
        if { CRTcache = correlatedRangeTraversals(e); CRTcache.nonEmpty }
        => CRTcache match {
              case Some(List(flatMap(Lambda(i,_),_),f2@flatMap(Lambda(j,b2),_),c))
                => optimize(subst(f2,Let(j,toExpr(i),b2),e))  // don't remove c
              case _ => apply(e,optimize)
           }
      case _ => apply(e,optimize)
    }

  def movePredicates ( qs: List[Qualifier] ): List[Qualifier]
    = qs match {
        case (p@Predicate(_))::r
          => movePredicates(r):+p
        case q::r
          => q::movePredicates(r)
        case Nil => Nil
      }

  def movePredicates ( e: Expr ): Expr
    = e match {
        case Comprehension(h,qs)
          => qs.span{ case GroupByQual(_,_) => false
                      case AssignQual(_,_) => false
                      case _ => true } match {
               case (r,q::s)
                 => val Comprehension(_,ss) = movePredicates(Comprehension(h,s))
                    Comprehension(movePredicates(h),
                                  movePredicates(r)++(q+:ss))
               case _ => Comprehension(movePredicates(h),movePredicates(qs))
             }
        case _ => apply(e,movePredicates)
      }

  def optimizeAll ( e: Expr ): Expr = {
    var olde = e
    var ne = olde//movePredicates(olde)
    do { olde = ne
         ne = normalizeAll(optimize(ne))
       } while (olde != ne)
    ne
  }
}
