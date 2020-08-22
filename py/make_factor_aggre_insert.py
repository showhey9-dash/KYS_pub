#!usr/bin/env python
#
# 各コース、条件毎の尚早結果を集計し、集計テーブルに登録する
# 集計用テーブル　： Aggre_summary_stock
# 集計ファクター　：　枠、騎手、調教師、父、父タイプ、BMS、BMSタイプ、ニックス、ニックスタイプ
# ファクター事集計SQL　：　Dropbox\Public\KeibaYosou\sql\202008modelに格納しているものをバインド変数化して
#                          sqlフォルダに新規作成
# コース一覧　　　：　N_COURSE

# import
import sys

import MySQLdb
import pymysql
import pandas as pd
import pandas.io.sql as psql
import numpy as np
# from pandas.tools.plotting import scatter_matrix
import matplotlib.pyplot as plt
import os
import sklearn.model_selection as ms
# 決定木
from sklearn.tree import DecisionTreeClassifier
from datetime import datetime, timedelta
from sklearn import tree

# DB接続
import mysql.connector as mysqlCon
db_config = {
    'host': 'localhost',
    'db': 'everydb',  # Database Name
    'user': 'root',
    'passwd': 'systemsss',
    'charset': 'utf8',
}

conn = MySQLdb.connect(host=db_config['host'], db=db_config['db'], user=db_config['user'],
                       passwd=db_config['passwd'], charset=db_config['charset'])

# from datetime import datetime
#conn = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['passwd'], db=db_config['db'])

# メイン処理
def execute():

    # コース一覧取得
    course_list = get_course_list()

    # コース一覧分Loop
    for i, row in course_list.iterrows():
        print(row)

        # 枠集計データ登録
        #aggre_waku(row)


        # 騎手集計データ登録
        #aggre_kisyu(row)

        # 調教師集計データ登録
        #aggre_chokyosi(row)

        # 血統　父集計データ登録
        #aggre_father(row)

        # 血統　父タイプ集計データ登録
        aggre_father_type(row)

        # 血統　BMS集計データ登録
        #aggre_bms(row)

        # 血統　BMSタイプ集計データ登録
        aggre_bms_type(row)

        # 血統　ニックス集計データ登録
        #aggre_knicks(row)

        # 血統　ニックスタイプ集計データ登録
        # aggre_knicks_type(row)

        # テスト用
        #if i ==3 :
        #    break

    print("処理終了")

# コース一覧取得
def get_course_list():
    try:
        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        sql = "select JyoCD, Kyori, TrackCD from n_course group by JyoCD, Kyori, TrackCD"
        # print(sql)
        # print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn, params={})

        # print("sql実行終了")
        # print(df)

        db.close()

        return df
    except Exception as e:
        print("get_course_list() レース一覧取得処理で例外発生")
        conn.close()
        print(e)
        return

# 集計データ登録
def  aggre_factor(fact_name, row, file_path ):

    # 引数から必要な情報を取り出す
    # 引用ママ要編集
    jyo_cd = row["JyoCD"]
    kyori = row['Kyori']
    track_cd = row['TrackCD']
    # print(jyo_cd)

    try:
        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        # file_path = "../sql/202008model/01_waku.sql"
        target_sql_file = open(file_path)
        sql = target_sql_file.read()  # 終端まで読み込んだdataを返却
        # print(sql)

        # print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        # insert文なので処理件数が返却
        # 検索開始年は2015年でベタ。綺麗なデータが登録されてからは現在年に切り替えればよい
        #df = psql.read_sql(sql, conn, params={"cond_year": "2015", "jyocd": jyo_cd, "kyori": kyori, "trackcd": track_cd})
        df = db.execute(sql, {'cond_year': '2015', 'jyocd': jyo_cd, 'kyori': kyori, 'trackcd': track_cd})
        # print("sql実行終了")
        # pd.set_option('display.max_columns', 100)
        print(df)
        conn.commit()

        db.close()

        return df
    except Exception as e:
        print(fact_name, "集計処理で例外発生")
        conn.close()
        print(e)
        return

        # 枠集計データ登録

def aggre_waku(row):
    print("aggre_waku")
    fact_name = "枠番"
    file_path = "../sql/202008model/01_waku.sql"
    aggre_factor(fact_name, row, file_path)

# 騎手集計データ登録
def aggre_kisyu(row):
    print("aggre_kisyu")
    fact_name = "騎手"
    file_path = "../sql/202008model/02_kisyu.sql"
    aggre_factor(fact_name, row, file_path)

# 調教師集計データ登録
def aggre_chokyosi(row):
    print("aggre_chokyosi")
    fact_name = "調教師"
    file_path = "../sql/202008model/03_chokyosi.sql"
    aggre_factor(fact_name, row, file_path)

# 血統　父集計データ登録
def aggre_father(row):
    print("aggre_father")
    fact_name = "父"
    file_path = "../sql/202008model/04_father.sql"
    aggre_factor(fact_name, row, file_path)

# 血統　父タイプ集計データ登録
def aggre_father_type(row):
    print("aggre_father_type")
    fact_name = "父タイプ"
    file_path = "../sql/202008model/05_father_type.sql"
    aggre_factor(fact_name, row, file_path)

# 血統　BMS集計データ登録
def aggre_bms(row):
    print("aggre_bms")
    fact_name = "BMS"
    file_path = "../sql/202008model/06_BMS.sql"
    aggre_factor(fact_name, row, file_path)

# 血統　BMSタイプ集計データ登録
def aggre_bms_type(row):
    print("aggre_bms_type")
    fact_name = "BMSタイプ"
    file_path = "../sql/202008model/07_BMS_type.sql"
    aggre_factor(fact_name, row, file_path)


# 血統　ニックス集計データ登録
def aggre_knicks(row):
    print("aggre_knicks")
    fact_name = "ニックス"
    file_path = "../sql/202008model/08_ニックス.sql"
    aggre_factor(fact_name, row, file_path)

# 血統　ニックスタイプ集計データ登録
def aggre_knicks_type(row):
    print("aggre_knick_type")
    fact_name = "ニックスタイプ"
    file_path = "../sql/202008model/09_ニックス_type.sql"
    aggre_factor(fact_name, row, file_path)


# 入力チェック
def inputChk(year):
    print('start--inputChk')
    # 空文字チェック
    if year == '':
        print("入力値に空文字があります")
        return False
    # 対象文字以外
    elif not year.isdigit() :
        print("入力値に数値以外があります")
        return False
    # 4桁チェック
    elif len(year) != 4 :
        print("入力値の桁数が不正です")
        return False
    else:
        return True

def main():
    # 引数をコマンドラインから入力
    # 集計開始年

    """
    year = input("Year?: ")

    if not inputChk(year):
        print('---end---')
        sys.exit()
    """
    # メイン処理を実行
    execute()


if __name__ == '__main__':
    main()