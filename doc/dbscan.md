阿基米德项目DBSCAN聚类算法应用案例
=============

*编写人：石友谊/郑宸*

*最后更新时间：2014.5.6*

##一、算法描述

###1.原理

#####DBSCAN中的的几个定义：



- **Ε领域**：给定对象半径为Ε内的区域称为该对象的Ε领域；
- **核心对象**：如果给定对象Ε领域内的样本点数大于等于MinPts，则称该对象为核心对象；
- **直接密度可达**：对于样本集合D，如果样本点q在p的Ε领域内，并且p为核心对象，那么对象q从对象p直接密度可达。
- **密度可达**：对于样本集合D，给定一串样本点p1,p2….pn，p= p1,q= pn,假如对象pi从pi-1直接密度可达，那么对象q从对象p密度可达。
- **密度相连**：存在样本集合D中的一点o，如果对象o到对象p和对象q都是密度可达的，那么p和q密度相联。

可以发现，密度可达是直接密度可达的传递闭包，并且这种关系是非对称的。密度相连是对称关系。DBSCAN目的是找到密度相连对象的最大集合

#####DBSCAN算法描述:

- 输入: 包含n个对象的数据库，半径e，最少数目MinPts;
- 输出:所有生成的簇，达到密度要求。

#####优点：

- 与K-means方法相比，DBSCAN不需要事先知道要形成的簇类的数量。
- 与K-means方法相比，DBSCAN可以发现任意形状的簇类。
- 同时，DBSCAN能够识别出噪声点。
DBSCAN对于数据库中样本的顺序不敏感，即Pattern的输入顺序对结果的影响不大。但是，对于处于簇类之间边界样本，可能会根据哪个簇类优先被探测到而其归属有所摆动。

#####缺点：

- DBScan不能很好反映高尺寸数据。
- DBScan不能很好反映数据集以变化的密度。

###2.伪代码：

	//输入：数据对象集合D，半径Eps，密度阈值MinPts
	//输出：聚类C
	DBSCAN（D, Eps, MinPts）
	Begin
		init C=0; //初始化簇的个数为0
		for each unvisited point p in D
			mark p as visited; //将p标记为已访问
			N = getNeighbours (p, Eps);
			if sizeOf(N) < MinPts then
				mark p as Noise; //如果满足sizeOf(N) < MinPts，则将p标记为噪声
			else
				C= next cluster; //建立新簇C
				ExpandCluster (p, N, C, Eps, MinPts);
			end if
		end for
	End
	
	//其中ExpandCluster算法伪码如下：
	ExpandCluster(p, N, C, Eps, MinPts)
		add p to cluster C; //首先将核心点加入C
		for each point p’ in N
			mark p' as visited;
			N’ = getNeighbours (p’, Eps); //对N邻域内的所有点在进行半径检查
			if sizeOf(N’) >= MinPts then
				N = N+N’; //如果大于MinPts，就扩展N的数目
			end if
			if p’ is not member of any cluster
				add p’ to cluster C; //将p' 加入簇C
			end if
		end for
	End ExpandCluster

###3.并行化方法

1. 给定距离函数，计算向量相似度矩阵。
	- 复杂度O(n²)。 Similarity Hashing做性能优化。

2. 通过epsilon阈值和minpoints限制提取近邻点。
	- 这一步生成图来表示近邻点。
	- 可以在这一步完成噪点过滤。

3. 在上一步产出的图上，找出联通部分。
    - Mapreduce/BSP

###4.文献

- [dbscan wikipedia](http://en.wikipedia.org/wiki/DBSCAN)
- [Density Based Distributed Clustering](http://www.dbs.ifi.lmu.de/Publikationen/Papers/EDBT_04_DBDC.pdf)
- [Scalable Density-Based Distributed Clustering](http://pdf.aminer.org/000/541/673/scalable_density_based_distributed_clustering.pdf)
- [similarity hashing](http://en.wikipedia.org/wiki/Locality-sensitive_hashing)

##二、具体实现及调用

###1. 输入数据结构及说明

###2. 输出数据结构及说明

###3. 算法调用语句示例


##三、案例描述

###1. 业务问题描述及分析

###2. 数据的准备

###3. 算法的运行及模型生成

###4。 模型的评估


##四、单机算法的对比

