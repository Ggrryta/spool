------------------------- MODULE ShardedWake ----------------------------
(***************************************************************************)
(* spool/mpsc.ShardedChan 唤醒协议的 TLA+ 规约                             *)
(*                                                                        *)
(* 建模范围（抽象层级）：                                                  *)
(*   - Out 通道与下游消费者抽象为无界汇（delivered），它们不参与唤醒协议    *)
(*   - 2 个 key（分片），每 key 最多 MaxPuts 次                            *)
(*   - sig 容量 1：Put/Close 的 trySend（满则丢弃）建模为 sig' = 1         *)
(*   - 泵的动作：PumpDrain（搬运一值）、PumpIdle（全空→入睡或退出）、       *)
(*     PumpWake（被信号唤醒）——泵"排空→检查→入睡"原子化为 PumpIdle         *)
(*                                                                        *)
(* 安全不变量：                                                            *)
(*   TypeOK / NoDup / NoLoss / PerKeyFIFO / ExitedClean / NoLostWakeup     *)
(* 时序性质（弱公平下）：                                                  *)
(*   DrainAndExit —— closing 后全部送达且泵退出                            *)
(*                                                                        *)
(* 历史注记：NoLostWakeup 正是 v0.2 实测死锁的机器检测器——当时的 bug       *)
(* 版本里 Close 只置 closing 不发信号，"泵睡在 sig 上且 sig=0 且 closing"   *)
(* 可达，违反本不变量，TLC 会在写代码前抓出该 bug。                         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANT MaxPuts    \* 每 key 最大 Put 次数

VARIABLES
  backlogs,    \* [key |-> 值序列]
  sig,         \* 唤醒信号（容量 1）
  sleeping,    \* 泵是否睡在 <-sig 上
  closing,
  pumpExited,
  putOrder,    \* 成功 Put 的全局顺序
  putCount,    \* [key |-> 已 Put 次数]
  delivered    \* 泵已搬运到 Out 的值

Keys == 1..2
Values == {<<k, i>> : k \in Keys, i \in 1..MaxPuts}
vars == <<backlogs, sig, sleeping, closing, pumpExited, putOrder,
          putCount, delivered>>

Init ==
  /\ backlogs = [k \in Keys |-> <<>>]
  /\ sig = 0
  /\ sleeping = FALSE
  /\ closing = FALSE
  /\ pumpExited = FALSE
  /\ putOrder = <<>>
  /\ putCount = [k \in Keys |-> 0]
  /\ delivered = <<>>

(*************************************************************************)
(* 生产者 Put：入分片 + trySignal。                                        *)
(* trySend 语义：sig=0 → 1；sig=1 → 丢弃（仍为 1）。两种情况均为 sig'=1。  *)
(*************************************************************************)
Put(k) ==
  /\ ~closing
  /\ putCount[k] < MaxPuts
  /\ backlogs' = [backlogs EXCEPT ![k] = Append(@, <<k, putCount[k] + 1>>)]
  /\ putCount' = [putCount EXCEPT ![k] = @ + 1]
  /\ putOrder' = Append(putOrder, <<k, putCount[k] + 1>>)
  /\ sig' = 1
  /\ UNCHANGED <<sleeping, closing, pumpExited, delivered>>

(* Close：置位 + 唤醒信号（v0.2 死锁修复：不发信号则违反 NoLostWakeup） *)
Close ==
  /\ ~closing
  /\ closing' = TRUE
  /\ sig' = 1
  /\ UNCHANGED <<backlogs, sleeping, pumpExited, putOrder, putCount, delivered>>

(* 泵：从分片 k 搬运一个值 *)
PumpDrain(k) ==
  /\ ~sleeping /\ ~pumpExited
  /\ backlogs[k] # <<>>
  /\ delivered' = Append(delivered, Head(backlogs[k]))
  /\ backlogs' = [backlogs EXCEPT ![k] = Tail(@)]
  /\ UNCHANGED <<sig, sleeping, closing, pumpExited, putOrder, putCount>>

(*************************************************************************)
(* 泵：全部分片空——closing 则退出，否则入睡。                              *)
(* 入睡时执行 <-sig：吸收可能已存在的信号（sig'=0），此后由 PumpWake 唤醒。 *)
(*************************************************************************)
PumpIdle ==
  /\ ~sleeping /\ ~pumpExited
  /\ \A k \in Keys : backlogs[k] = <<>>
  /\ IF closing
       THEN /\ pumpExited' = TRUE
            /\ UNCHANGED <<sig, sleeping>>
       ELSE /\ sleeping' = TRUE
            /\ sig' = 0
  /\ UNCHANGED <<backlogs, closing, delivered, putOrder, putCount>>

(* 泵：被信号唤醒，重新扫描 *)
PumpWake ==
  /\ sleeping /\ ~pumpExited /\ sig = 1
  /\ sleeping' = FALSE
  /\ sig' = 0
  /\ UNCHANGED <<backlogs, closing, pumpExited, delivered, putOrder, putCount>>

Next ==
  \/ \E k \in Keys : Put(k)
  \/ Close
  \/ \E k \in Keys : PumpDrain(k)
  \/ PumpIdle \/ PumpWake

Spec == Init /\ [][Next]_vars
FairSpec ==
  Spec
  /\ \A k \in Keys : WF_vars(PumpDrain(k))
  /\ WF_vars(PumpIdle) /\ WF_vars(PumpWake)

(************************ 不变量（safety） ********************************)
TypeOK ==
  /\ backlogs \in [Keys -> Seq(Values)]
  /\ sig \in 0..1
  /\ sleeping \in BOOLEAN
  /\ closing \in BOOLEAN
  /\ pumpExited \in BOOLEAN
  /\ putOrder \in Seq(Values)
  /\ delivered \in Seq(Values)
  /\ putCount \in [Keys -> 0..MaxPuts]

(* 无重复送达 *)
NoDup ==
  \A i, j \in DOMAIN delivered : i # j => delivered[i] # delivered[j]

(* 无丢失：每个成功 Put 的值要么已搬运，要么仍在某分片中 *)
NoLoss ==
  \A i \in DOMAIN putOrder :
    \/ \E j \in DOMAIN delivered : delivered[j] = putOrder[i]
    \/ \E k \in Keys, j \in DOMAIN backlogs[k] :
         backlogs[k][j] = putOrder[i]

(* 同 key 严格 FIFO：delivered 中同 key 值的序号单调递增 *)
PerKeyFIFO ==
  \A i, j \in DOMAIN delivered :
    (i < j /\ delivered[i][1] = delivered[j][1]) =>
      delivered[i][2] < delivered[j][2]

(* 泵退出时必然已关闭且全部分片排空 *)
ExitedClean ==
  pumpExited => (closing /\ \A k \in Keys : backlogs[k] = <<>>)

(*************************************************************************)
(* 唤醒协议核心安全性质：泵无信号入睡（sig=0）当且仅当确实无事可做。        *)
(* 若违反（睡而未醒却有数据/该退出），即为丢唤醒——协议级死锁。             *)
(*************************************************************************)
NoLostWakeup ==
  (sleeping /\ sig = 0) =>
    (\A k \in Keys : backlogs[k] = <<>> /\ ~closing)

(************************ 时序性质（liveness） ****************************)
(* closing 后（配合公平性）最终全部送达且泵退出 *)
DrainAndExit ==
  (<>[] closing) =>
    (<>[] Len(delivered) = Len(putOrder) /\ pumpExited)

=============================================================================
