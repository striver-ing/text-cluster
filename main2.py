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
CLUSTER_BUFFER_ZISE = 100
PAGE_SIZE = 1000

cluster_buffer = {
        # "hot_id":{'title':'xxxx', 'article_ids':[1,2,3,4], 'article_count':0},
        # "hot_id":{'title':'xxxx', 'article_ids':[1,2,3,4], 'article_count':0}
}
db = OracleDB()

def deal_cluster_buffer():
    global cluster_buffer
    cluster_buffer_tota_count = len(cluster_buffer)
    cluster_buffer_deal_count = 0
    for hot_id, data in cluster_buffer.items():
        tools.print_loading('缓存到达最大限制 正在向数据库中写数据 %d/%d'%(cluster_buffer_deal_count, cluster_buffer_tota_count))
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

        cluster_buffer_deal_count += 1

    # 清空缓存
    cluster_buffer = {}
    tools.print_loading(' '*100)

def main():
    rownum = 0

    sql = 'select count(*) from tab_iopm_article_info'
    result = db.find(sql)
    articles_count = result[0][0]
    deal_count = 0

    while articles_count:

        # 查文章
        sql = '''
            select *
              from (select rownum r, id, title
                      from tab_iopm_article_info
                     where rownum <= %d and info_type != 3 and release_time >= to_date('2017-10-10 00:00:00', 'yyyy-mm-dd hh24:mi:ss') and release_time <= to_date('2017-10-25 23:59:59', 'yyyy-mm-dd hh24:mi:ss'))
             where r > %d
        '''%(rownum + PAGE_SIZE, rownum)

        sql = '''
            select *
              from (select rownum r, id, title
                      from tab_iopm_article_info
                     where rownum <= %d and info_type != 3)
             where r > %d
        '''%(rownum + PAGE_SIZE, rownum)

        articles = db.find(sql)
        if not articles:
            deal_cluster_buffer()
            break

        rownum += PAGE_SIZE

        # 查热点
        sql = 'select id, title, hot from tab_iopm_hot_info'
        hots = db.find(sql)
        # 将元组的结果转换为列表
        for i, hot in enumerate(hots):
            hots[i] = list(hot)

        # 查询类别最大id
        sql = 'select max(id) from tab_iopm_hot_info'
        result = db.find(sql)
        max_hot_id = result[0][0] if result[0][0] else 0

        for article in articles:
            max_similar = {'similarity':0, 'hot_id':-1, 'article_id':-1, 'hot_title':'', 'article_count':0, 'hot_pos':-1}  # 最相似的文章 similarity表示相似度(0~1)
            article_id = article[1]
            article_text = article[2]

            for i, hot in enumerate(hots):
                hot_id = hot[0]
                hot_text = hot[1]
                # article_count = hot[2]

                similarity = compare_text(hot_text, article_text)
                # print('''
                #     article_text %s
                #     hot_text     %s
                #     similarity   %s
                #     '''%(article_text, hot_text, similarity))

                # 将相似的文章和热点的信息记录下来
                if similarity > max_similar['similarity']:
                    max_similar['similarity'] = similarity
                    max_similar['hot_id'] = hot_id
                    max_similar['article_id'] = article_id
                    max_similar['hot_title'] = article_text if len(hot_text) > len(article_text) else hot_text
                    max_similar['hot_pos'] = i # 相似热点的下标 后续根据下标来更新热点的标题和文章数


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

                hots[max_similar['hot_pos']][1] = max_similar['hot_title'] # 热点标题
                hots[max_similar['hot_pos']][2] += 1  # 热点文章信息量
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
                deal_cluster_buffer()

                # 查热点
                sql = 'select id, title, hot from tab_iopm_hot_info'
                hots = db.find(sql)
                # 将元组的结果转换为列表
                for i, hot in enumerate(hots):
                    hots[i] = list(hot)

if __name__ == '__main__':
    main()