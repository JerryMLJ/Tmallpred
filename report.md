> **梅李杰 231220125**
> 13721512405@163.com
> **小组信息**：个人实现

## 任务一：数据清洗与用户统计特征提取

### 程序设计主要流程

+ **环境初始化与日志屏蔽**：构建`SparkConf`配置本地/集群运行环境，初始化`JavaSparkContext`，并防止`INFO`日志刷屏设置日志级别为`WARN`
+ **数据清洗，过滤不合法数据**：利用`filter`算子对输入测试集解析、排除非法数据
+ **计算全局最大时间戳**
+ **用户行为特征聚合**
+ **特征空间转换与极值搜索**
+ **Min-Max归一化**
+ **伪代码如下**：
```java
1.  rawData  =sc.textFile(inputPath)
2.  cleanData=rawData.filter(字段数=5且behavior∈{pv,cart,fav,buy})
3.  maxTime  =cleanData.map(取timestamp).reduce(Max)
4.  userPairs=cleanData.mapToPair(行 => (userId, (timestamp, 1, score)))
        // score: buy=4,cart=3,fav=2,pv=1
5.  userFeat =userPairs.reduceByKey((a,b) => (max(a.t, b.t), a.cnt+b.cnt, a.scr+b.scr))
6.  vectors  =userFeat.mapValues((t,cnt,scr)=> (
        F1=(maxTime - t)/86400,
        F2=count
        F3=score
    ))
7.  (minV,maxV)=vectors.values().reduce(各维度Min, Max)
8.  result=vectors.map(
        norm_Fi=(Fi - min_i) / (max_i - min_i),若max_i=min_i则填0
        输出 "userId, F1, F2, F3"
    )
9.  result.saveAsTextFile(outputPath)
```
### 程序采用主要算法
+ **数据清洗**：利用$Spark$的`filter()`过滤非法记录，过滤条件包括空记录，字段数量小于5，行为类型不符
+ **用户特征聚合**：首先将每条行为映射为`(user_id,(timestamp,1,score))`然后利用`reduceByKey()`聚合最终得到`(user_id,(latestTime,totalCount,totalScore))`
+ **Min-Max归一化**：采用公式`F'=(F−Fmin)/(Fmax−Fmin)`
### 优化及其效果
+ **RDD缓存**：对于重复访问的数据使用`cache()`缓存，提高运行效率
+ **调试日志模块**：为了方便程序验证，输出调试
### 程序性能分析
整个任务主要包括过滤、映射、聚合和归一化四个阶段，整体时间复杂度为`O(N)`,空间复杂度为`O(U)`，具有良好扩展性
### 程序运行截图
![[Pasted image 20260616172548.png|646]]
![[Pasted image 20260616175050.png|510]]
![[Pasted image 20260616175105.png|510]]
## 任务二：基于行为标签的倒排索引和共现分析

### 程序设计主要流程
**读取用户行为数据** ->**过滤深度交互** ->**构建商品-用户键值对** ->**groupByKey()** ->**生成倒排索引**
							->**groupByKey()** ->**用户商品集合去重** ->**两两商品组合** ->**reduceByKey()统计频次** ->**输出商品共现矩阵** 
+ **伪代码如下**：
```java
1.  cleanData=sc.textFile(inputPath).map(按逗号分割)
2.  deepInter=cleanData.filter(behavior ∈ {cart, fav, buy})
3.  // 倒排索引:item→user列表
    itemUserPair=deepInter.mapToPair(行 => (itemId, userId)).distinct()
    invertIndex =itemUserPair.groupByKey().mapValues(users => users.拼接(","))
    输出: "itemId \t user1,user2,..."
4.  // 共现计算
    userItemPair  =deepInter.mapToPair(行 => (userId, itemId)).distinct()
    userItemsGroup=userItemPair.groupByKey()
    itemCoOccur=userItemsGroup.flatMapToPair(每个用户):
        对该用户的所有商品去重，两两配对
        统一顺序: "较小ID,较大ID" → 计数1
    .reduceByKey(求和)
5.  top1000=itemCoOccur.按共现次数降序排列.take(1000)
    输出: "itemA,itemB \t count"
6.  保存结果
```
### 程序采用主要算法
+ **商品倒排索引构建**：筛选行为，`groupByKey()`算子聚合形成映射
+ **商品共现矩阵构建**：将行为转化为`(userId,itemId`)键值对，分组、去重，`reduceByKey()`统计共现次数
### 优化及其效果
+ **重复交互去重优化**：程序利用`HashSet`对用户商品集合进行去重，避免重复统计，提高准确率
+ **倒排索引去重优化**：在构建`(itemId,userId)`键值对后，使用`distinct()`去除重复记录
### 程序性能分析
+ 总体空间复杂度为`O(Uk^2)`,空间复杂度约为`O(U+I)`
### 程序运行截图
![[Pasted image 20260616172738.png]]

## 任务三：K-Means用户聚类

### 程序设计主要流程
**读取用户特征数据** ->**解析用户三维特征向量** ->**随机抽样初始化聚类中心** ->**Brroadcast广播聚类中心** ->**计算用户与各聚类中心欧式距离** ->**aggregateByKey()统计并更新聚类中心** ->**判断是否收敛** ->**输出聚类中心及用户聚类标签**
+ **伪代码如下**：
```java
1.  读取user_features.csv → (userId, [F1, F2, F3])
2.  // 初始化
    K=3,maxIter=20,ε=1e-4
    centers=随机抽 K个用户的特征向量作为初始中心
3.  // K-Means迭代
    for iter in 1..maxIter:
        广播 centers 到所有节点
        // 分配簇:每个点找最近中心
        clusterSums=userFeatures.map(点 => (最近中心ID, (特征向量, 计数1)))
        // 聚合:同一簇特征求和、计数求和
        newClusterSums=clusterSums.aggregateByKey(
            zeroValue=(0向量, 计数0),
            seqOp    =累加特征和计数,
            combOp   =合并分区间结果
        )
        // 计算新中心=特征和/计数
        newCenters=newClusterSums.collectAsMap().各簇求平均值
        // 检查收敛:新旧中心最大偏移量
        maxShift=max(各中心偏移)
        if maxShift < ε: break
        else: centers=newCenters
4.  输出聚类中心: "cluster_id \t F1, F2, F3"
5.  重新分配所有用户到最近簇
    输出用户标签: "user_id, cluster_id"
```
### 程序采用主要算法
+ **K-Means聚类算法**：随机选取K个用户作为初始聚类中心，通过迭代不断更新聚类中心，最终完成用户聚类
+ **欧式距离计算**：采用三维用户特征向量之间的欧氏距离衡量相似程度，并划分用户
+ **聚类中心更新**：利用`aggregateByKey()`统计各簇分量并计算平均值，再计算均值作为新的聚类中心直至聚类中心收敛
### 优化及其效果
+ **Broadcast广播优化**：每轮迭代广播聚类中心，减少网络通信开销，提高聚类效率
+ **aggregateBykey聚合优化**：采用`aggregateByKey()`同时完成特征累加和样本计数，提高并行计算性能
+ **联合分量聚合优化**：通过自定义`NewClass`实体类，精简程序

### 程序性能分析
空间复杂度约为`O(N+K)`，时间复杂度约为`O(N*K*T)`,N位用户数量，K为聚类数，T为迭代次数

### 程序运行截图
![[Pasted image 20260616172752.png]]

## 任务四：基于二分图随机游走的PersonalRank推荐

### 程序设计主要流程
**读取用户行为数据** ->**过滤深度交互行为并去重** ->**构建User-Item二分图** ->**统计User、Item出度并分配概率** ->**合并边形成图** ->**初始化节点PR值** ->**通过join和reduceByKey实现PR迭代更新** -> **过滤目标用户已交互商品** ->**排序输出Top10结果**
+ **伪代码如下**：
```java
1.  读取 clean_behavior_log.csv
    过滤 behavior∈{cart, fav, buy}
    映射为 (U_userId,I_itemId)，去重
2.  获取目标用户已交互商品列表，广播
3.  // 构建二分图
    user→item边: groupByKey → 每条边概率=1/用户出度
    item→user边: 反转后groupByKey → 每条边概率=1/商品出度
    graphEdges=两边合并
    allNodes=图中所有唯一节点，初始值0.0
4.  // 初始化PR值
    rankRDD=allNodes.map(目标用户=1.0,其他=0.0)
5.  // PersonalRank 迭代
    for iter in 1..maxIter:
        newRank=rankRDD.join(graphEdges)
                        .map(终点节点 => 当前PR × 转移概率)
        rankRDD=newRank.reduceByKey(求和)
                         .rightOuterJoin(allNodes)
                         .mapValues(流入值 × α + 若为目标用户则 +(1-α))

6.  // 筛选推荐
    candidates=rankRDD.filter(节点以"I_"开头且不在已交互列表中)
    按PR值降序排列，取Top-10
    去前缀输出: "targetUserId \t item1:score,item2:score,..."
```
### 程序采用主要算法
+ **二分图构建**：根据深度交互行为构建`(user,item)`键值对，统计出度，构建二分图
+ **PersonalRank随机游走算法**：每轮迭代，通过`join()`将当前节点`Rank`与邻接边关联，并将`Rank`传播至相邻节点，利用`reduceByKey()`聚合贡献。
$$PR(v)=\alpha \sum_{u \in In(v)} \frac{PR(u)}{|Out(u)|} + (1 - \alpha) \cdot r_v$$
### 优化及其效果
+ **交互记录去重优化**：使用`distinct()`去除重复交互记录，提高准确性
+ **节点右外连接对齐优化**：在`PR`迭代过程中，仅通过`reduceByKey()`聚合容易导致后续节点丢失，破坏完整图结构，因此在每轮迭代结束后使用`rightOuterJoin()`对未收到共线值的接二点自动补零
### 程序性能分析
总体时间复杂度为`O(T*E)`，空间复杂度为`O(U+I+E)`,`U`为用户数，`I`为商品数，`E`为二分图边数，`T`为迭代次数