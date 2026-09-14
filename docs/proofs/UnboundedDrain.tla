------------------------- MODULE UnboundedDrain -------------------------
(***************************************************************************)
(* spool/unbounded 关闭与排空协议的 TLA+ 规约                              *)
(*                                                                        *)
(* 建模范围（抽象层级）：                                                  *)
(*   - 值为全局唯一二元组 <<producer, seq>>                                *)
(*   - 信号通道 chan 容量 1（直递 + 通知），对应实现的 c chan T            *)
(*   - 2 个生产者、每个最多 MaxPuts 次（控制状态空间规模）                 *)
(*   - Close 可在任意时刻发生（对应用户 Close 或 ctx 取消）                *)
(*   - putOrder 记录成功 Put 的全局顺序，用于 FIFO 检查                    *)
(*                                                                        *)
(* 安全不变量：                                                            *)
(*   TypeOK / ChanCap / NoDup / FIFO / NoLoss / ClosedClean /              *)
(*   ClosedComplete                                                       *)
(* 时序性质（弱公平下）：                                                  *)
(*   AllDeliveredAfterClose —— closing 后已入队值最终全部送达              *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANT MaxPuts    \* 每生产者最大 Put 次数

VARIABLES
  backlog,     \* 内部积压队列
  chan,        \* 容量 1 信号通道的内容
  closing,
  closed,
  putOrder,    \* 成功 Put 的全局顺序
  putCount,    \* [producer |-> 已 Put 次数]
  delivered,   \* 消费者已接收的值
  sawClosed    \* 消费者观察到通道关闭

Producers == 1..2
Values == {<<p, i>> : p \in Producers, i \in 1..MaxPuts}
vars == <<backlog, chan, closing, closed, putOrder, putCount,
          delivered, sawClosed>>

Init ==
  /\ backlog = <<>>
  /\ chan = <<>>
  /\ closing = FALSE
  /\ closed = FALSE
  /\ putOrder = <<>>
  /\ putCount = [p \in Producers |-> 0]
  /\ delivered = <<>>
  /\ sawClosed = FALSE

(*************************************************************************)
(* 生产者 Put：镜像实现的两个分支——backlog 空且通道空则直递，否则入 backlog *)
(*************************************************************************)
Put(p) ==
  /\ ~closing
  /\ putCount[p] < MaxPuts
  /\ LET v == <<p, putCount[p] + 1>>
     IN /\ IF backlog = <<>> /\ chan = <<>>
           THEN /\ chan' = <<v>>
                /\ backlog' = backlog
           ELSE /\ backlog' = Append(backlog, v)
                /\ chan' = chan
        /\ putOrder' = Append(putOrder, v)
        /\ putCount' = [putCount EXCEPT ![p] = @ + 1]
  /\ UNCHANGED <<closing, closed, delivered, sawClosed>>

(*************************************************************************)
(* 消费者 Load：backlog 非空且通道空则搬运队首值                           *)
(*************************************************************************)
LoadMove ==
  /\ backlog # <<>>
  /\ chan = <<>>
  /\ chan' = <<Head(backlog)>>
  /\ backlog' = Tail(backlog)
  /\ UNCHANGED <<closing, closed, putOrder, putCount, delivered, sawClosed>>

(*************************************************************************)
(* 消费者 Load：backlog 空且 closing 则关闭通道                           *)
(*************************************************************************)
LoadClose ==
  /\ backlog = <<>>
  /\ closing
  /\ ~closed
  /\ closed' = TRUE
  /\ UNCHANGED <<backlog, chan, closing, putOrder, putCount, delivered, sawClosed>>

(* 消费者接收数据（通道中有值时优先于观察到关闭） *)
RecvData ==
  /\ chan # <<>>
  /\ delivered' = Append(delivered, Head(chan))
  /\ chan' = Tail(chan)
  /\ UNCHANGED <<backlog, closing, closed, putOrder, putCount, sawClosed>>

(* 消费者观察到通道关闭 *)
RecvClosed ==
  /\ chan = <<>>
  /\ closed
  /\ sawClosed' = TRUE
  /\ UNCHANGED <<backlog, chan, closing, closed, putOrder, putCount, delivered>>

(* Close：幂等由 ~closing 前置保证；backlog 空则立即关闭通道 *)
Close ==
  /\ ~closing
  /\ closing' = TRUE
  /\ IF backlog = <<>> THEN closed' = TRUE ELSE closed' = FALSE
  /\ UNCHANGED <<backlog, chan, delivered, sawClosed, putOrder, putCount>>

(* 终态自环：观察到关闭后协议完成（保持死锁检查语义干净） *)
Done ==
  /\ sawClosed
  /\ UNCHANGED vars

Next ==
  \/ \E p \in Producers : Put(p)
  \/ LoadMove \/ LoadClose \/ RecvData \/ RecvClosed \/ Close \/ Done

Spec == Init /\ [][Next]_vars
FairSpec ==
  Spec
  /\ WF_vars(LoadMove) /\ WF_vars(LoadClose)
  /\ WF_vars(RecvData) /\ WF_vars(RecvClosed)

(************************ 不变量（safety） ********************************)
TypeOK ==
  /\ backlog \in Seq(Values)
  /\ chan \in Seq(Values)
  /\ putOrder \in Seq(Values)
  /\ delivered \in Seq(Values)
  /\ putCount \in [Producers -> 0..MaxPuts]
  /\ closing \in BOOLEAN
  /\ closed \in BOOLEAN
  /\ sawClosed \in BOOLEAN

(* 通道容量恒为 1 *)
ChanCap == Len(chan) <= 1

(* 无重复送达 *)
NoDup ==
  \A i, j \in DOMAIN delivered : i # j => delivered[i] # delivered[j]

(* 全局 FIFO：接收顺序 = Put 提交顺序 *)
FIFO ==
  \A i \in DOMAIN delivered : delivered[i] = putOrder[i]

(* 无丢失：每个成功 Put 的值要么已送达，要么仍在 backlog/chan 中 *)
NoLoss ==
  \A i \in DOMAIN putOrder :
    \/ \E j \in DOMAIN delivered : delivered[j] = putOrder[i]
    \/ \E j \in DOMAIN backlog    : backlog[j]    = putOrder[i]
    \/ \E j \in DOMAIN chan       : chan[j]       = putOrder[i]

(* 通道关闭时 backlog 必然已排空 *)
ClosedClean == closed => backlog = <<>>

(* 观察到关闭时，全部成功 Put 的值必然已送达 *)
ClosedComplete == sawClosed => Len(delivered) = Len(putOrder)

(************************ 时序性质（liveness） ****************************)
(* closing 后（配合公平性）最终全部送达 *)
AllDeliveredAfterClose ==
  (<>[] closing) => (<>[] (Len(delivered) = Len(putOrder)))

=============================================================================
