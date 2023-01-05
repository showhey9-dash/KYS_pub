# TFの印1~8と結果コメントを新規テーブル（n_tf_shirushi_and_comment）にInsertする
# 【前提】事前にInsert_Yoso_Shirushi.pyを実行し、各馬の予想印データがある事
# TF データ取得対象
#  結果コメント・・（root）\TFJV\MY_DATA\KEK_COM\yyyy\KC(場CD)(yy)(回次)(日次).DAT　　　　　　　例）KC092269.DAT
#               　  行：テータ登録が行われた順（順不同）
#                 　列：(場CD)(yy)(回次)(日次)(R番)(馬番),（コメント）　　　　　　　例）0922691106,駐立悪い
#  新規テーブル「n_tf_shirushi_and_comment」定義
#              主キー：(場CD)(yy)(回次)(日次)(R番)(馬番)の複合、year、monthday、kettonum、印１～８、コメント、レースレベル、登録日
#  ※ヘッダ無し
#
# 【手順】
# ①入力された期間にある開催情報を取得
# ②古いデータを削除
# ③TFからテキストファイルをロード
# ④テーブルにInsert

from ast import Pass
import math
import sys

import MySQLdb
import pymysql
import pandas as pd
import pandas.io.sql as psql
import numpy as np
import requests
import subprocess
import configparser
from sqlalchemy import create_engine
import os

# DB接続
import mysql.connector as mysqlCon
db_config = {
    'host': 'localhost',
    'db': 'everydb',  # Database Name
    'user': 'root',
    'passwd': 'systemP3',
    'charset': 'utf8',
}

conn = MySQLdb.connect(host=db_config['host'], db=db_config['db'], user=db_config['user'],
                       passwd=db_config['passwd'], charset=db_config['charset'])

# テーブル登録用Connection
conn2 = create_engine('mysql+mysqlconnector://root:systemP3@localhost:3306/everydb')


inif = configparser.ConfigParser()
inif.read('./config.ini', 'UTF-8')   # コマンドプロンプトから実行するときはpyを.に変更（開発中は./py/）
tf_kekka_comment_path = inif.get('csv', 'tf_kekka_comment_path')

# メイン処理（年、開始月日、　終了月日）
def execute(year, startday, endday):
    pass

    # 開催情報取得
    kaisai_list = get_kaisai_list(year, startday, endday)

    # 開催リスト分繰り返し
    for i, row in kaisai_list.iterrows():

        # 開催該当ファイルを開く
        # ファイル内のデータをDataFrame形式で取得する
        comment_list = get_comment_list(row)

        if comment_list is None:
            continue
        
        # 値をバラシてn_tf_shirushi_and_commentへのupdateデータを作成する
        update_n_tf_shirushi_and_comment(comment_list)


# ファイル内のデータを取得する
def get_comment_list(row):

    # rowからファイル名を生成      KC(場CD)(yy)(回次)(日次).DAT
    year = row['Year']

    jyoCd = row['JyoCD']
    yearYY = year[2:]
    kaiji = row['Kaiji'][1:]
    nichiji_ = row['Nichiji']
    # 日次が英語の場合があるので考慮する
    try:
        nichiji = str(int(nichiji_))
    except:
        pass

    file_name = "KC" + jyoCd + yearYY + kaiji + nichiji +".DAT"

    file_path = tf_kekka_comment_path + year + "\\" + file_name

    print(file_path)

    # ファイルオープン
    if os.path.exists(file_path):
        with open(file_path) as f:
            lines = f.readlines()
    else:
        # 空文字
        return None
    
    return lines

# n_tf_shirushi_and_commentへのupdateデータを作成する
def update_n_tf_shirushi_and_comment(comment_list):
    
    # ファイルの行数分繰り返し
    # データ型はList
    for comment_data in comment_list:
        # race_id_list
        # race_id_list = comment_data[0]

        target = ','
        idx = comment_data.find(target) 
        comment_b = comment_data[idx + 1:]
        comment = comment_b.replace("\n", "")

        jyoCD_l = comment_data[0:2]
        yearYY_l = comment_data[2:4]
        kaiji_l = comment_data[4:5]
        nichiji_l = "0" + comment_data[5:6]
        racenum_l = comment_data[6:8]
        umaban_l = comment_data[8:10]
 
        try:

            print(yearYY_l, "," , jyoCD_l, "," , kaiji_l , ",", nichiji_l, ",", racenum_l, ",", umaban_l)

            # データ登録用DataRrame作成
            db = conn.cursor(MySQLdb.cursors.DictCursor)
            sql = 'update n_tf_shirushi_and_comment set KekkaComment = %s where YearYY = %s and JyoCD = %s and Kaiji = %s and nichiji = %s and RaceNum = %s and Umaban = %s '
            db.execute(sql,(comment, yearYY_l, jyoCD_l, kaiji_l, nichiji_l, racenum_l, umaban_l))
            conn.commit()
        
        except:
            pass
        


    # DFリスト分のデータを登録(出走頭数？)
    # for i, df_t in comment_list.iterrows():


# 開催情報取得（年、開始月日、終了月日）
def get_kaisai_list(year, startday, endday):

    db = conn.cursor(MySQLdb.cursors.DictCursor)
    # 開催一覧にレース番号は不要。ファイルの中に混在しているから
    sql = 'select distinct Year, MonthDay, JyoCD, Kaiji, Nichiji from n_race r where year = %(year)s and  ' \
          'MonthDay >= %(startday)s and MonthDay <= %(endday)s and JyoCD <= "10" order by year, MonthDay, JyoCD '
    # db.execute(sql,(row['year'], row['startday'], row['endday']))
    df = psql.read_sql(sql, conn,
                           params={"year": year, "startday": startday, 'endday': endday})

    # print("df　：　対象レース")
    # print(df)
    db.close()

    return df

# 実行用 コマンドライン引数で 年,月日,現在時間を入力
def main():

    args = sys.argv

    
    year = args[1]
    startday = args[2]
    endday = args[3]
    '''
    year = '2022'
    startday = '1225'
    endday = '1225'
    '''

    print(" year: " + year + " startday :" + startday + " endday :" + endday)

    # メイン処理を実行
    try:
        execute(year, startday, endday)
    except Exception as e:
        print("execute() 全体処理で例外発生")
        conn.close()
        print(e)
        return e


if __name__ == '__main__':
    main()