# -*- coding: utf-8 -*-
'''
Created on 2017-11-02 10:35
---------
@summary: 文本聚类调度函数
---------
@author: Boris
'''

from cluster.compare_text import compare_text
from db.oracledb import OracleDB
import utils.tools as tools

SIMILARITY = 0.45 # 相似度 聚类阈值  相似度大于 n 就算一类 0<=n<=1
CLUSTER_BUFFER_ZISE = 10000
PAGE_SIZE = 50000

def main():
    db = OracleDB()
    cluster_buffer = {
        # "hot_id":{'title':'xxxx', 'article_ids':[1,2,3,4], 'article_count':0},
        # "hot_id":{'title':'xxxx', 'article_ids':[1,2,3,4], 'article_count':0}
    }

    rownum = 0
    while True:

        # 查文章
        sql = '''
            select *
              from (select rownum r, id, title
                      from tab_iopm_article_info
                     where rownum <= %d and release_time >= to_date('2017-10-10 00:00:00', 'yyyy-mm-dd hh24:mi:ss') and release_time <= to_date('2017-10-25 23:59:59', 'yyyy-mm-dd hh24:mi:ss') and info_type != 3 )
             where r > %d
        '''%(rownum, rownum + PAGE_SIZE)

        articles = db.find(sql)

        rownum += PAGE_SIZE

        articles_count = len(articles)
        deal_count = 0

        # 查热点
        sql = 'select id, title, hot from tab_iopm_hot_info'
        hots = db.find(sql)

        # 查询类别最大id
        sql = 'select max(id) from tab_iopm_hot_info'
        result = db.find(sql)
        max_hot_id = result[0][0] if result[0][0] else 0

        for article in articles:
            max_similar = {'similarity':0, 'hot_id':-1, 'article_id':-1, 'hot_title':'', 'article_count':0}  # 最相似的文章 similarity表示相似度(0~1)
            article_id = article[1]
            article_text = article[2]

            for i, hot in enumerate(hots):
                hot_id = hot[0]
                hot_text = hot[1]
                # article_count = hot[2]

                similarity = compare_text(hot_text, article_text)
                print('''
                    article_text %s
                    hot_text     %s
                    similarity   %s
                    '''%(article_text, hot_text, similarity))

                # 将相似的文章和热点的信息记录下来， 此处一旦找到符合的类别则跳出循环，不在往下比较。此处用到假设法 如A和B相似，A和C相似，那么B和C相似。但是库中的类别B和C不是同一类，所以A找到B后、不可能在找到更相似的类别、所以跳出循环
                if similarity >= SIMILARITY:
                    max_similar['similarity'] = similarity
                    max_similar['hot_id'] = hot_id
                    max_similar['article_id'] = article_id
                    max_similar['hot_title'] = article_text if len(hot_text) > len(article_text) else hot_text
                    # 更新下缓存中的热点标题 以短标题为准
                    hots[i][1] = max_similar['hot_title']
                    hots[i][2] += 1 # 文章数

                    max_similar['article_count'] = hot[2]

                    break

            # 该舆情找到了所属类别
            if max_similar['similarity'] >= SIMILARITY:
                # 将热点及舆情信息缓存起来
                if max_similar['hot_id'] not in cluster_buffer.keys():
                    cluster_buffer[max_similar['hot_id']] = {
                        'title':'', 'article_ids':[], 'article_count':0
                    }

                cluster_buffer[max_similar['hot_id']]['title'] = max_similar['hot_title']
                cluster_buffer[max_similar['hot_id']]['article_count'] = max_similar['article_count']
                cluster_buffer[max_similar['hot_id']]['article_ids'].append(max_similar['article_id'])

            else:
                # 在原有的类别集合中添加新的类别
                max_hot_id += 1
                hots.append([max_hot_id, article_text, 1]) # 1 为文章数

                # 文章自己是一类， 自己和自己肯定相似，所以在聚类的缓存中把自己及类别对应关系缓存起来
                cluster_buffer[max_hot_id] = {
                    'title':article_text,
                    'article_ids':[article_id],
                    'article_count':1
                }

            deal_count += 1
            tools.print_loading('正在聚类分析 已完成 %d/%d'%(deal_count, articles_count))

            # 如果大于最大缓存，则添加到数据库中
            if len(cluster_buffer) > CLUSTER_BUFFER_ZISE:
                for hot_id, data in cluster_buffer.items():
                    article_ids = data['article_ids']
                    article_count = data['article_count']
                    hot_title = data['title'].replace("'", "''")

                    # 更新线索对应的热点id
                    sql = 'update tab_iopm_article_info set hot_id = %s where id in (%s)'%(hot_id, str(article_ids).replace('[', '').replace(']', ''))
                    db.update(sql)

                    # 查询库中热点信息存在 存在更新，不存在插入
                    sql = 'select id from tab_iopm_hot_info where id = %s'%hot_id
                    if db.find(sql):
                        # 更新热点文章数量
                        sql = "update tab_iopm_hot_info set hot = %d, title = '%s' where id = %s"%(article_count, hot_title, hot_id)
                        db.update(sql)
                    else:
                        sql = "insert into tab_iopm_hot_info (id, title, hot) values (%s, '%s', %s)"%(hot_id, hot_title, article_count)
                        db.add(sql)

                    # 清空缓存
                    cluster_buffer = {}

                    # 查热点
                    sql = 'select id, title, hot from tab_iopm_hot_info'
                    hots = db.find(sql)

if __name__ == '__main__':
    main()
