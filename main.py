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

def main():
    db = OracleDB()

    # 查文章
    sql = '''
        select *
          from (select rownum r, id, title
                  from tab_iopm_article_info
                 where rownum >= 1)
         where r <= 100000
    '''
    articles = db.find(sql)

    # 查热点
    sql = 'select id, title from tab_iopm_hot_info'
    hots = db.find(sql)

    for article in articles:
        max_similar = {'similarity':0, 'hot_id':-1, 'article_id':-1, 'hot_title':''}  # 最相似的文章 similarity表示相似度(0~1)
        article_id = article[1]
        article_text = article[2]

        for hot in hots:
            hot_id = hot[0]
            hot_text = hot[1]

            similarity = compare_text(hot_text, article_text)
            # print('''
            #     article_text %s
            #     hot_text     %s
            #     similarity   %s
            #     '''%(article_text, hot_text, similarity))
            if similarity > max_similar['similarity']:
                max_similar['similarity'] = similarity
                max_similar['hot_id'] = hot_id
                max_similar['article_id'] = article_id
                max_similar['hot_title'] = article_text if len(hot_text) > len(article_text) else hot_text

        if max_similar['similarity'] > SIMILARITY:
            sql = 'update tab_iopm_article_info set hot_id = %s where id = %s'%(max_similar['hot_id'], max_similar['article_id'])
            db.update(sql)
            sql = "update tab_iopm_hot_info set hot = hot + 1, title = '%s' where id = %s"%(max_similar['hot_title'], max_similar['hot_id'])
            db.update(sql)

        else:
            sql = 'select sequence.nextval from dual'
            hot_id = db.find(sql)[0][0]
            sql = "insert into tab_iopm_hot_info (id, title, hot) values (%s, '%s', 1)"%(hot_id, article_text)
            db.add(sql)
            sql = 'update tab_iopm_article_info set hot_id = %s where id = %s'%(hot_id, article_id)
            db.update(sql)

        sql = 'select id, title from tab_iopm_hot_info'
        hots = db.find(sql)


if __name__ == '__main__':
    main()