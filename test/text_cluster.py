#!/usr/bin/env python
# coding=utf-8

import sys,os

import numpy as np
from numpy import *
import jieba
import math
import jieba.analyse
from pprint import pprint
# jieba.load_userdict("userdict.txt")

def read_from_file(file_name):
    with open(file_name,"r", encoding = 'utf-8') as fp:
        words = fp.read()
    return words
def stop_words(stop_word_file):
    words = read_from_file(stop_word_file)
    result = jieba.cut(words)
    new_words = []
    for r in result:
        new_words.append(r)
    return set(new_words)

def gen_sim(A,B):
    '''
    @summary: 取两点间距离
    ---------
    @param A:
    @param B:
    ---------
    @result:
    '''

    num = float(np.dot(A,B.T))
    denum = np.linalg.norm(A) * np.linalg.norm(B)
    if denum == 0:
        denum = 1
    cosn = num / denum
    sim = 0.5 + 0.5 * cosn
    return sim

def del_stop_words(words,stop_words_set):
    result = jieba.cut(words)
    new_words = []
    for r in result:
        if r not in stop_words_set:
            new_words.append(r)
            #print r.encode("utf-8"),
    #print len(new_words),len(set(new_words))
    return new_words

def tfidf(term,doc,word_dict,docset):
    '''
    @summary:
    ---------
    @param term:词语
    @param doc: 该篇文章
    @param word_dict: 词语字典 {'您好'：2} 您好在整个文本集合出现两次
    @param docset: 文本集合
    ---------
    @result:
    '''

    tf = float(doc.count(term)) / (len(doc) + 0.001)
    idf = math.log(float(len(docset)) / word_dict[term])
    return tf * idf

def idf(term,word_dict,docset):
    '''
    @summary: lg(文档数量/该词在所有文档中出现的次数)
    ---------
    @param term: 词语
    @param word_dict: 词语字典 {'您好'：2} 您好在整个文本集合出现两次
    @param docset: 文本集合
    ---------
    @result:
    '''

    idf = math.log(float(len(docset)) / word_dict[term])
    return idf

def word_in_docs(word_set,docs):
    '''
    @summary: 返回词频字典  {'哈哈': 1, '你好': 2}
    ---------
    @param word_set: 关键词
    @param docs: 文本集合
    ---------
    @result:
    '''

    word_dict = {}
    for word in word_set:
        word_dict[word] = len([doc for doc in docs if word in doc])
    return word_dict

def get_all_vector(file_path,stop_words_set):
    '''
    @summary: 词袋模型  tf-idf
    用结巴分词 提取关键词  保留前20个
    ---------
    @param file_path:
    @param stop_words_set:
    ---------
    @result:
    '''

    names = [ os.path.join(file_path,f) for f in os.listdir(file_path) ]
    posts = [ open(name).read() for name in names ]
    docs = []
    word_set = set()
    for post in posts:
        doc = del_stop_words(post,stop_words_set)
        docs.append(doc)
        word_set |= set(doc) # 取并集
        #print len(doc),len(word_set)

    word_set = list(word_set)
    print(word_set)
    docs_vsm = []
    #for word in word_set[:30]:
        #print word.encode("utf-8"),
    for doc in docs:
        temp_vector = []
        for word in word_set:
            temp_vector.append(doc.count(word) * 1.0)
        #print temp_vector[-30:-1]
        docs_vsm.append(temp_vector)

    docs_matrix = np.array(docs_vsm)
    print(docs_matrix)
    #print docs_matrix.shape
    #print len(np.nonzero(docs_matrix[:,3])[0])
    column_sum = [ float(len(np.nonzero(docs_matrix[:,i])[0])) for i in range(docs_matrix.shape[1]) ]
    column_sum = np.array(column_sum)
    column_sum = docs_matrix.shape[0] / column_sum
    idf =  np.log(column_sum)
    idf =  np.diag(idf)
    #print idf.shape
    #row_sum    = [ docs_matrix[i].sum() for i in range(docs_matrix.shape[0]) ]
    #print idf
    #print column_sum
    for doc_v in docs_matrix:
        if doc_v.sum() == 0:
            doc_v = doc_v / 1
        else:
            doc_v = doc_v / (doc_v.sum())


    tfidf = np.dot(docs_matrix,idf)

    return names,tfidf

def randCent(dataSet, k):
    n = shape(dataSet)[1] #返回：一个整型数字的元组，元组中的每个元素表示相应的数组每一维的长度
    centroids = mat(zeros((k,n)))#create centroid mat
    for j in range(n):#create random cluster centers, within bounds of each dimension
        minJ = min(dataSet[:,j])
        rangeJ = float(max(dataSet[:,j]) - minJ)
        centroids[:,j] = mat(minJ + rangeJ * random.rand(k,1))
    return centroids

def kMeans(dataSet, k, distMeas=gen_sim, createCent=randCent):
    m = shape(dataSet)[0]
    clusterAssment = mat(zeros((m,2)))#create mat to assign data points
                                      #to a centroid, also holds SE of each point
    centroids = createCent(dataSet, k)
    clusterChanged = True
    while clusterChanged:
        clusterChanged = False
        for i in range(m):#for each data point assign it to the closest centroid
            minDist = inf;
            minIndex = -1
            for j in range(k):
                distJI = distMeas(centroids[j,:],dataSet[i,:])
                if distJI < minDist:
                    minDist = distJI;
                    minIndex = j
            if clusterAssment[i,0] != minIndex:
                # clusterChanged = True
                pass
            clusterAssment[i,:] = minIndex,minDist**2
        #print centroids
        for cent in range(k):#recalculate centroids
            ptsInClust = dataSet[nonzero(clusterAssment[:,0].A==cent)[0]]#get all the point in this cluster
            centroids[cent,:] = mean(ptsInClust, axis=0) #assign centroid to mean
    return centroids, clusterAssment



if __name__ == "__main__":
    stop_words = stop_words("./stop_words.txt")
    names,tfidf_mat = get_all_vector("./chinese/",stop_words)
    print(names)
    print(tfidf_mat)
    myCentroids,clustAssing = kMeans(tfidf_mat, 3, gen_sim, randCent)
    for label,name in zip(clustAssing[:,0],names):
        print (label,name)


