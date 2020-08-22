#!usr/bin/env python
#
# 該当するレースの傾向をファクター毎に判断する
# Web上で観れるようにするため、参照用のテーブルにインサートする
# 当該レース情報　：　s_race
# 馬場状態取得SQL ：　90_データ集計考察用.sql
# 集計用テーブル　： Aggre_summary_stock
# データ表示方法　：　DJangoを使ったWebアプリ or AppSheetからの参照
# 判断結果登録テーブル　：　（新規作成）？AppSheet使うならGoogleDriveにスプレッドシートに出力する


# import
import sys

import MySQLdb
import pymysql
import pandas as pd
import pandas.io.sql as psql
import numpy as np
import requests
"""
# from pandas.tools.plotting import scatter_matrix
import matplotlib.pyplot as plt
import os
import sklearn.model_selection as ms
# 決定木
from sklearn.tree import DecisionTreeClassifier
from datetime import datetime, timedelta
from sklearn import tree
"""
from sqlalchemy import create_engine

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

# テーブル登録用Connection
conn2 = create_engine('mysql+mysqlconnector://root:systemsss@localhost:3306/everydb')

# 通知用接続情報
url = "https://notify-api.line.me/api/notify"
access_token = 'KbbLSpCku2T7ctRykF2JTq1SrnOmxMwTbeosobi4Ymu'
headers = {'Authorization': 'Bearer ' + access_token}

def execute(year, monthday, time):

    print("処理開始")
    # 時間を基に対象レースを取得
    target_race = get_race_list(year, monthday, time)

    # print(target_race)

    # 馬場状態取得（全量）
    baba_info = get_baba_info(year, monthday)

    # 対象レース分繰り返し
    for i, row in target_race.iterrows():

        # 既存データ削除
        delete_matched_race(row)

        # 対象レースに出走する馬情報の取得
        syussouba_list = get_syussouba_list(row)

        # レース傾向取得
        race_trand_list = get_race_trand(row, baba_info)

        # 各馬毎にファクターに一致するか判断していく
        # 判断結果をaggre_summary_stockにinsert
        for j, row2 in syussouba_list.iterrows():
            # 対象レースの傾向を取得
            print(row2['JyoCD'])
            print(row2['RaceNum'])
            print(row2['Umaban'])

            match_trand = pd.DataFrame(np.ndarray(9).reshape(1,9), dtype="object",
                                       index=['row_0'], columns=['waku', 'kisyu', 'cyokyosi', 'father', 'father_type',
                                                                 'bms', 'bms_type', 'knicks', 'knicks_type'])
            # print(match_trand.dtypes)

            # 枠
            match_trand.at['row_0', "waku"] = get_match_factor(row2, race_trand_list, "Wakuban", "枠")

            # 騎手
            match_trand.at['row_0', 'kisyu'] = get_match_factor(row2, race_trand_list, "KisyuRyakusyo", "騎手")

            # 調教師
            match_trand.at['row_0', 'chokyosi'] = get_match_factor(row2, race_trand_list, "ChokyosiRyakusyo", "調教師")

            # 父
            match_trand.at['row_0', 'father'] = get_match_factor(row2, race_trand_list, "Father", "調教師")

            # 父タイプ
            match_trand.at['row_0', 'father_type'] = get_match_factor(row2, race_trand_list, "FatherType", "種牡馬タイプ")

            # BMS
            match_trand.at['row_0', 'bms'] = get_match_factor(row2, race_trand_list, "Bms", "BMS")

            # BMSタイプ
            match_trand.at['row_0', 'bms_type'] = get_match_factor(row2,  race_trand_list, "BmsType", "BMSタイプ")

            # ニックス
            match_trand.at['row_0', 'knicks'] = get_match_knicks(row2, race_trand_list, "ニックス")

            # ニックスタイプ
            match_trand.at['row_0', 'knicks_type'] = ""

            # 蓄積情報をXXXに登録（追加する）

            # 合致結果をテーブルにUPSERT
            upsert_match_factor(row2, match_trand)

            print("該当馬傾向抽出終了")

    message1 = "処理が完了しました。PowerBIのデータセットを更新してください。\n"
    message = message1
    payload = {'message' : message}
    requests.post(url, headers=headers, params=payload)

    print("全処理終了")

# 該当条件で既に傾向抽出しているデータを削除する
# レース単位
def delete_matched_race(row):

    db = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = 'delete from aggre_jadge_stock where Year = %s and MonthDay = %s and JyoCD = %s and RaceNum = %s'
    db.execute(sql,(row['Year'], row['MonthDay'], row['JyoCD'], row['RaceNum']))
    conn.commit()

# 当該馬の傾向合致内容をテーブルに登録（UPSERT）
def upsert_match_factor(race, match):

    try:
        pass
        # データ登録用DataRrame作成
        upsert_data = pd.DataFrame({'Year':race['Year'], 'MonthDay':race['MonthDay'],'JyoCD':race['JyoCD'],
                                    'RaceNum':race['RaceNum'], 'Umaban':race['Umaban'], 'Bamei':race['Bamei'],
                                    'Wakuban':match['waku'], 'Kisyu':match['kisyu'],'Chokyosi':match['chokyosi'],
                                    'Father':match['father'], 'FatherType':match['father_type'], 'BMS':match['bms'],
                                    'BMSType':match['bms_type'],'Knicks':match['knicks'], 'KnicksType':match['knicks_type']})
        # print(upsert_data)
        # UPSERT処理
        tablename = "aggre_jadge_stock"
        upsert_data.to_sql(tablename, conn2, if_exists='append', index=False,
                           index_label={'Year', 'MonthDay', 'JyoCD', 'RaceNum', 'Umaban'})


    except Exception as e:
        print("upsert_match_factor() 当該馬傾向合致登録処理で例外発生")
        conn.close()
        print(e)
        payload = {'message': e }
        requests.post(url, headers=headers, params=payload)
        return e


# ----------- ファクター別傾向合致結果抽出 --------------------------
# 汎用的傾向_合致結果抽出
def get_match_factor(row2, race_trand_list, column_name, factor):

    column = row2[column_name]
    # print(column )

    row3 = race_trand_list[(race_trand_list["Factor"] == factor) & (race_trand_list["FactorValue"] == column)]
    # print(row3)

    if row3.empty:
        return_value = ""
    else:
        rate = row3["hukusyo_rate_2"].values[0]

        # print(rate)
        return_value = factor + " " + column + ": " + str(rate) + "%"
        print(return_value)
    return return_value

# ニックス傾向_合致結果抽出
def get_match_knicks(row2, race_trand_list, factor):

    # 種牡馬名とBMSを結合
    column = row2["Father"] + "×" + row2["Bms"]
    print(column)

    row3 = race_trand_list[(race_trand_list["Factor"] == factor) & (race_trand_list["FactorValue"] == column)]
    # print(row3)

    if row3.empty:
        return_value = ""
    else:
        rate = row3["hukusyo_rate_2"].values[0]

        # print(rate)
        return_value = factor + " " + column + ": " + str(rate) + "%"
        print(return_value)
    return return_value

# ニックスタイプ傾向_合致結果抽出
def get_match_knicks_type(row2, race_trand_list):
    pass

# ----------- 抽出条件作成準備  ---------------------
# 馬場コード取得
def get_baba_cd(jyocd, trackcd, baba_info):
    # print(baba_info)

    # row_a = baba_info[baba_info['JyoCD'] == jyocd]
    # row_b = row_a.head(1)
    # print(type(row_b))

    if trackcd < "20":
        baba_cd = baba_info.at[jyocd, 'AtoSibaBabaCD']
    else:
        baba_cd = baba_info.at[jyocd, 'AtoDirtBabaCD']
    # print(baba_cd)

    return baba_cd

# レース傾向取得
# row（場コード、トラックコード、距離、馬場状態コード）をもとに出走馬一覧を取得
def get_race_trand(row, baba_info):
    jyocd = row["JyoCD"]
    trackcd = row["TrackCD"]
    kyori = row["Kyori"]

    # 馬場コード取得
    babacd = get_baba_cd(jyocd, trackcd, baba_info)

    # 傾向取得
    try:
        # print("処理")
        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        filePath = "../sql/202008model/70_summary_1.sql"
        targetSqlFile = open(filePath)
        sql = targetSqlFile.read()  #
        # print(sql)
        # print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn, params={"jyocd": jyocd, "trackcd": trackcd, "kyori": kyori, "babacd": babacd})

        # print("sql実行終了")
        # print(df)

        db.close()

        return df
    except Exception as e:
        print("get_race_trand() 当該レース傾向取得処理で例外発生")
        conn.close()
        print(e)
        payload = {'message': e }
        requests.post(url, headers=headers, params=payload)
        return e


# 出走馬リスト取得
# row（年、月日、場コード、レース番号）をもとに出走馬一覧を取得
def get_syussouba_list(row):

    year = row["Year"]
    monthday = row["MonthDay"]
    jyocd = row["JyoCD"]
    raceNum = row["RaceNum"]

    try:
        # print("処理")
        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        filePath = "../sql/202008model/41_get_syussouba_list.sql"
        targetSqlFile = open(filePath)
        sql = targetSqlFile.read()  #
        # print(sql)
        # print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn, params={"year": year, "monthday": monthday, "jyocd": jyocd, "racenum":raceNum })

        # print("sql実行終了")
        # print(df)

        db.close()

        return df
    except Exception as e:
        print("get_syussouba_list() 当該レース出走馬取得処理で例外発生")
        conn.close()
        print(e)
        payload = {'message': e }
        requests.post(url, headers=headers, params=payload)
        return e



# 当該レースを取得
# 年、月日、時間
def get_race_list(year, monthday, time):
    try:
        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        time_plus3 = str(int(time) + 300)
        print(time_plus3)

        # 抽出レース　現在時刻以降　3時間以内
        sql = "select * from s_race r where Year = %(year)s and r.MonthDay = %(monthday)s " \
              " and (r.RaceNum = '11' or ( r.HassoTime > %(time)s and r.HassoTime < %(time_plus3)s )); "
        # print(sql)
        # print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn, params={"year": year, "monthday": monthday, 'time':time, "time_plus3": time_plus3})

        # print("sql実行終了")
        # print(df)

        db.close()

        return df
    except Exception as e:
        print("get_race_list() 当該レース一覧取得処理で例外発生")
        conn.close()
        print(e)
        payload = {'message': e }
        requests.post(url, headers=headers, params=payload)
        return e

# 年月日を基に現在の馬場状態を取得
def get_baba_info(year, monthday):

    try:
        # print("処理")

        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        filePath = "../sql/202008model/40_get_baba_condition.sql"
        targetSqlFile = open(filePath)
        sql = targetSqlFile.read()  #

        # print(sql)
        # print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn,
                           params={"year": year, "monthday": monthday, "year_2": year, "monthday_2": monthday,
                                   "year_3": year, "monthday_3": monthday, "year_4": year, "monthday_4": monthday,
                                   "year_5": year, "monthday_5": monthday})

        # JyoCDをindexに使用
        df2 = df.copy()
        df2.index = df['JyoCD']

        # print("sql実行終了")
        print(df2)

        db.close()

        return df2

    except Exception as e:
        print("get_baba_info() 馬場状態取得処理で例外発生")
        conn.close()
        print(e)
        payload = {'message': e }
        requests.post(url, headers=headers, params=payload)
        return e


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

    args = sys.argv

    year = args[1]
    monthday = args[2]
    time = args[3]

    print(" year: " + year + " monthday :" + monthday + " time :" + time)

    """    
    year = input("Year?: ")

    if not inputChk(year):
        print('---end---')
        sys.exit()
    """

    # テストデータ
    # year = "2020"
    # monthday = "0815"
    # time = "0900"

    # メイン処理を実行
    execute(year, monthday, time)



if __name__ == '__main__':
    main()
