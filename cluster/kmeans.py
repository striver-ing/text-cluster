# -*- coding: utf-8 -*-
'''
Created on 2017-11-01 16:20
---------
@summary:
---------
@author: Boris
'''
import sys
sys.path.append('../')
import init
from numpy import *
import numpy as np
from cluster.cut_text import CutText
import utils.tools as tools

def distEclud(vecA, vecB):
    return sqrt(sum(power(vecA - vecB, 2))) #la.norm(vecA-vecB)

def randCent(dataSet, k):
    print('dataSet shape', shape(dataSet))
    n = shape(dataSet)[1] #返回：一个整型数字的元组，元组中的每个元素表示相应的数组每一维的长度
    centroids = mat(zeros((k,n)))#create centroid mat
    for j in range(n):#create random cluster centers, within bounds of each dimension
        print(dataSet[:,j])
        minJ = min(dataSet[:,j])
        rangeJ = float(max(dataSet[:,j]) - minJ)
        centroids[:,j] = mat(minJ + rangeJ * random.rand(k,1))
    return centroids

#kMeans算法
def kMeans(dataSet,k,distMeas=distEclud,createCent=randCent):
    #数据点的行数
    m=shape(dataSet)[0]
    #用于记录数据点到之心的距离平方
    clusterAssment=mat(zeros((m,2)))
    #中心点
    centroids=createCent(dataSet,k)
    #聚类结束标志
    clusterChanged=True
    while clusterChanged:
        clusterChanged=False;
        #遍历每条数据
        for i in range(m):
            #设置两个变量，分别存放数据点到质心的距离，及数据点属于哪个质心
            minDist=inf;minIndex=-1;
            #遍历每个质心
            for j in range(k):
                distJI=distMeas(centroids[j,:],dataSet[i,:])
                if(distJI<minDist):
                    #将数据归为最近的质心
                    minDist=distJI;minIndex=j;
            #簇分配结果发生变化，更新标志
            if clusterAssment[i,0]!=minIndex:clusterChanged=True
            clusterAssment[i,:]=minIndex,minDist**2
        # print (centroids)
        #更新质心
        for cent in range(k):
            ptsInClust=dataSet[nonzero(clusterAssment[:,0].A==cent)[0]]
            centroids[cent,:]=mean(ptsInClust,axis=0)

    return centroids,clusterAssment

#二分K-均值
def biKmeans(dataSet, k, distMeas=distEclud):
    """
    :param dataSet:
    :param k:
    :param distMeas:
    :return:
    选择一个初始的簇中心(取均值),加入簇中心列表
    计算每个数据点到簇中心的距离
    当簇的个数小于指定的k时
    对已经存在的每个簇进行2-均值划分,并计算其划分后总的SSE,找到最小的划分簇
    增加一个簇幷更新数据点的簇聚类
    度量聚类算法效果的指标是SSE(Sum of Squard Error, 误差平方和),也就是前面介绍的计算数据点与质心的欧氏距离的平方和.SSE越小说明数据点越接近它们的质心,聚类效果越好.
    """
    m = shape(dataSet)[0]
    clusterAssment = mat(zeros((m, 2)))
    # 创建一个初始簇, 取每一维的平均值
    centroid0 = mean(dataSet, axis=0).tolist()[0]
    centList = [centroid0]  # 记录有几个簇
    for j in range(m):
        clusterAssment[j, 1] = distMeas(mat(centroid0), dataSet[j, :]) ** 2
    while len(centList) < k:
        lowestSSE = inf
        # 找到对所有簇中单个簇进行2-means可以是所有簇的sse最小的簇
        for i in range(len(centList)):
            # 属于第i簇的数据
            ptsInCurrCluster = dataSet[nonzero(clusterAssment[:, 0].A == i)[0], :]
            # print ptsInCurrCluster
            # 对第i簇进行2-means
            centroidMat, splitClustAss = kMeans(ptsInCurrCluster, 2, distMeas)
            # 第i簇2-means的sse值
            sseSplit = sum(splitClustAss[:, 1])
            # 不属于第i簇的sse值
            sseNotSplit = sum(clusterAssment[nonzero(clusterAssment[:, 0].A != i), 1])
            if (sseSplit + sseNotSplit) < lowestSSE:
                bestCentToSplit = i
                bestNewCents = centroidMat
                bestClustAss = splitClustAss.copy()
                lowestSSE = sseNotSplit + sseSplit
        # 更新簇的分配结果
        #新增的簇编号
        bestClustAss[nonzero(bestClustAss[:, 0].A == 1)[0], 0] = len(centList)
        #另一个编号改为被分割的簇的编号
        bestClustAss[nonzero(bestClustAss[:, 0].A == 0)[0], 0] = bestCentToSplit  #
        # 更新被分割的的编号的簇的质心
        centList[bestCentToSplit] = bestNewCents[0, :].tolist()[0]
        # 添加新的簇质心
        centList.append(bestNewCents[1, :].tolist()[0])
        # 更新原来的cluster assment
        clusterAssment[nonzero(clusterAssment[:, 0].A == bestCentToSplit)[0], :] = bestClustAss
    return mat(centList), clusterAssment # mat(centList) 中心点；  clusterAssment 类别 距离

def get_all_vector(texts, cut_text):
    '''
    @summary: 词袋模型  tf-idf
    用结巴分词 提取关键词  保留前20个
    ---------
    @param texts: 文本集合[(id, text),(id, text),(id, text),(id, text)]
    @param cut_text: 拆词方法
    ---------
    @result:
    '''

    ids = [text[0] for text in texts]
    texts_info = [text[1] for text in texts]

    docs = []
    word_set = set()
    for text_info in texts_info:
        doc = cut_text(text_info, top_keyword_count = 10)
        docs.append(doc)
        word_set |= set(doc) # 取并集

    word_set = list(word_set)
    # print(word_set)

    docs_vsm = []
    for doc in docs:
        temp_vector = []
        for word in word_set:
            temp_vector.append(doc.count(word) * 1.0)
        docs_vsm.append(temp_vector)

    docs_matrix = np.array(docs_vsm)
    column_sum = [ float(len(np.nonzero(docs_matrix[:,i])[0])) for i in range(docs_matrix.shape[1]) ]
    column_sum = np.array(column_sum)
    column_sum = docs_matrix.shape[0] / column_sum
    idf =  np.log(column_sum)
    idf =  np.diag(idf)

    for doc_v in docs_matrix:
        if doc_v.sum() == 0:
            doc_v = doc_v / 1
        else:
            doc_v = doc_v / (doc_v.sum())

    tfidf = np.dot(docs_matrix,idf)
    with open('1.txt', 'w') as file:
        file.write(str(word_set))
        file.write(str(tfidf))

    return texts, tfidf


def cluster(texts, k = 2):
    '''
    @summary:
    ---------
    @param texts:
    texts = [
        (1, '毛羽司长：电视剧创作应该坚持以人民为中心'),
        (2, '毛羽司长：电视剧创作应该坚持以人民为中心'),
        (3, '毛羽司长指出: 电视剧创作应坚持以人民为中心 '),
        (4, '省级产业集聚区+电影学院 横店打造全球最强影视产业基地')
    ]
    ---------
    @result:
    '''

    # k = int(sqrt(len(texts) / 2))
    # print('k = ', k)

    cut_text = CutText()
    cut_text.set_stop_words('./cluster/stop_words.txt')

    ids,tfidf_mat = get_all_vector(texts, cut_text.cut_for_keyword)
    print('tfidf_mat shape', shape(tfidf_mat))

    myCentroids,clustAssing = biKmeans(tfidf_mat, k, distEclud)
    clustAssing = asarray(clustAssing)

    clust_json = {
        # 'lable':[{'text_id':1, 'text':'xxx', 'distince':2.0},{}],
        # 'lable2':[{'text_id':1, 'text':'xxx', 'distince':2.0},{}],
    }

    for clust in zip(clustAssing, ids):
        lable = clust[0][0] # 类别标签
        distince = clust[0][1] # 相异度
        text_id = clust[1][0] # 文本id
        text = clust[1][1] #文本内容

        if lable not in clust_json.keys():
            clust_json[lable] = []
        clust_json.get(lable).append({'text_id':text_id, 'text':text, 'distince':distince})

    for lable, clust in clust_json.items():
        clust_json[lable] = sorted(clust,  key=lambda text: text['distince'])  # 每一类的文本按照中心距离排序

    return clust_json

if __name__ == "__main__":
    texts = [
        (1, '毛羽司长：电视剧创作应该坚持以人民为中心'),
        (2, '毛羽司长：电视剧创作应该坚持以人民为中心'),
        (3, '毛羽司长指出: 电视剧创作应坚持以人民为中心 '),
        (4, '省级产业集聚区+电影学院 横店打造全球最强影视产业基地')
    ]

    result = cluster(texts, k = 2)
    print(tools.dumps_json(result))
