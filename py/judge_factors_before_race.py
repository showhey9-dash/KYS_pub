#!usr/bin/env python
#
# 該当するレースの傾向をファクター毎に判断する
# Web上で観れるようにするため、参照用のテーブルにインサートする
# 当該レース情報　：　s_race
# 馬場状態取得SQL ：　90_データ集計考察用.sql
# 集計用テーブル　： Aggre_summary_stock
# データ表示方法　：　DJangoを使ったWebアプリ or AppSheetからの参照
# 判断結果登録テーブル　：　（新規作成）？AppSheet使うならGoogleDriveにスプレッドシートに出力する
#                           20200820 PowerBIに表示


# import
import sys

import MySQLdb
import pymysql
import pandas as pd
import pandas.io.sql as psql
import numpy as np
import requests
import subprocess
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
        delete_uma_evaluation(row)

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

            # 20210626 各ファクターの値のみを入れる列を追加
            match_trand = pd.DataFrame(np.ndarray(17).reshape(1,17), dtype="object",
                                       index=['row_0'], columns=['waku', 'kisyu', 'cyokyosi', 'father', 'father_type',
                                                                 'bms', 'bms_type', 'knicks', 'knicks_type',
                                                                 'waku_v', 'kisyu_v', 'chokyosi_v', 'father_v',
                                                                 'father_type_v', 'bms_v', 'bms_Type_v', 'knicks_v'])
            # print(match_trand.dtypes)

            # 枠
            match_trand.at['row_0', "waku"] = get_match_factor(row2, race_trand_list, "Wakuban", "枠")

            # 騎手
            match_trand.at['row_0', 'kisyu'] = get_match_factor(row2, race_trand_list, "KisyuRyakusyo", "騎手")

            # 調教師
            match_trand.at['row_0', 'chokyosi'] = get_match_factor(row2, race_trand_list, "ChokyosiRyakusyo", "調教師")

            # 父
            match_trand.at['row_0', 'father'] = get_match_factor(row2, race_trand_list, "Father", "種牡馬")

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

            # 枠番_値
            match_trand.at['row_0', "waku_v"] = get_match_factor_v(row2, race_trand_list, "Wakuban", "枠")

            # 騎手_値
            match_trand.at['row_0', 'kisyu_v'] = get_match_factor_v(row2, race_trand_list, "KisyuRyakusyo", "騎手")

            # 調教師_値
            match_trand.at['row_0', 'chokyosi_v'] = get_match_factor_v(row2, race_trand_list, "ChokyosiRyakusyo", "調教師")

            # 種牡馬_値
            match_trand.at['row_0', 'father_v'] = get_match_factor_v(row2, race_trand_list, "Father", "種牡馬")

            # 種牡馬タイプ_値
            match_trand.at['row_0', 'father_type_v'] = get_match_factor_v(row2, race_trand_list, "FatherType", "種牡馬タイプ")

            # BMS_値
            match_trand.at['row_0', 'bms_v'] = get_match_factor_v(row2, race_trand_list, "Bms", "BMS")

            # BMSタイプ_値
            match_trand.at['row_0', 'bms_type_v'] = get_match_factor_v(row2, race_trand_list, "BmsType", "BMSタイプ")

            # ニックス_値
            match_trand.at['row_0', 'knicks_v'] = get_match_knicks_v(row2, race_trand_list, "ニックス")

            # 合致結果をテーブルにINSERT
            upsert_match_factor(row2, match_trand)

            print("該当馬傾向抽出終了")

        # 出走馬負荷情報作成（オッズ、人気、馬体重、増減差、対戦型マイニング
        # オッズ、人気取得
        uma_eva_list = get_uma_odds(row)

        # 馬体重取得
        uma_eva_list = get_bataijyu(uma_eva_list, row)

        # 対戦型マイニング取得
        uma_eva_list = get_taisengata_mining(uma_eva_list, row)

        # 対戦型マイニング人気、１位差算出処理

        # 算出結果をXXXテーブルにINSERT
        insert_uma_evaluation(row, uma_eva_list)

    # UiPath実行
    # subprocess.run([r"C:\Users\彰平\AppData\Local\UiPath\app-20.4.3\UiRobot.exe",
    #                 r"--file 'C:\UiPath\PowerBI_new_publish\Main.xaml'"])

    message1 = "傾向取得処理が完了しました。引き続き予想タイム処理の実行をお待ちください。\n"
    message = message1
    payload = {'message': message}
    requests.post(url, headers=headers, params=payload)

    print("全処理終了")

# 該当条件で既に傾向抽出しているデータを削除する
# 傾向該当情報（レース単位）
def delete_matched_race(row):

    db = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = 'delete from aggre_jadge_stock where Year = %s and MonthDay = %s and JyoCD = %s and RaceNum = %s'
    db.execute(sql,(row['Year'], row['MonthDay'], row['JyoCD'], row['RaceNum']))
    conn.commit()

# 該当条件で既に登録しているデータを削除する
def delete_uma_evaluation(row):

    db = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = 'delete from aggre_uma_evaluation where Year = %s and MonthDay = %s and JyoCD = %s and RaceNum = %s'
    db.execute(sql,(row['Year'], row['MonthDay'], row['JyoCD'], row['RaceNum']))
    conn.commit()


# 当該馬の傾向合致内容をテーブルに登録（UPSERT）
def upsert_match_factor(race, match):

    try:
        # データ登録用DataFrame作成
        upsert_data = pd.DataFrame({'Year':race['Year'], 'MonthDay':race['MonthDay'],'JyoCD':race['JyoCD'],
                                    'RaceNum':race['RaceNum'], 'Umaban':race['Umaban'], 'Bamei':race['Bamei'],
                                    'Wakuban':match['waku'], 'Kisyu':match['kisyu'],'Chokyosi':match['chokyosi'],
                                    'Father':match['father'], 'FatherType':match['father_type'], 'BMS':match['bms'],
                                    'BMSType':match['bms_type'],'Knicks':match['knicks'], 'KnicksType':match['knicks_type'],
                                    'Wakuban_val': match['waku_v'], 'Kisyu_val': match['kisyu_v'], 'Chokyosi_val': match['chokyosi_v'],
                                    'Father_val': match['father_v'], 'FatherType_val': match['father_type_v'],
                                    'BMS_val': match['bms_v'], 'BMSType_val': match['bms_type_v'], 'Knicks_val': match['knicks_v']
                                    })
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

# ----------- 馬評価算出処理 ----------------------------------------
# 馬オッズ取得
def get_uma_odds(row):
    year = row["Year"]
    monthday = row["MonthDay"]
    jyocd = row["JyoCD"]
    raceNum = row["RaceNum"]

    try:
        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        # 抽出レース　
        sql = "select Year, MonthDay, JyoCD, RaceNum, Umaban, (Cast(TanOdds as decimal)/10) as Odds, " \
              "TanNinki as Ninki from s_odds_tanpuku where Year = %(year)s and MonthDay = %(monthday)s " \
              " and JyoCD = %(jyocd)s and  RaceNum = %(raceNum)s ; "
        # print(sql)
        # print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn,
                           params={"year": year, "monthday": monthday, 'jyocd': jyocd, "raceNum": raceNum})

        # print("sql実行終了")
        # print(df)

        db.close()

        return df
    except Exception as e:
        print("get_uma_odds() 当該レースオッズ取得処理で例外発生")
        conn.close()
        print(e)
        payload = {'message': e}
        requests.post(url, headers=headers, params=payload)
        return e

# 馬体重情報取得
def get_bataijyu(eva_list, row):
    year = row["Year"]
    monthday = row["MonthDay"]
    jyocd = row["JyoCD"]
    racenum = row["RaceNum"]

    try:
        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        # 抽出レース
        sql = "select * from s_bataijyu where Year = %(year)s and MonthDay = %(monthday)s " \
              " and JyoCD = %(jyocd)s and  RaceNum = %(racenum)s ; "
        # print(sql)
        # print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn,
                           params={"year": year, "monthday": monthday, 'jyocd': jyocd, "racenum": racenum})

        # print("sql実行終了")
        # print(df)

        db.close()

        col1 = 7
        col2 = 8
        col3 = 9

        if df.empty:
            eva_list.at[:, "bataijyu"] = ""
            eva_list.at[:, "zogensa"] = ""
            return eva_list

        for i, row3 in eva_list.iterrows():

            col1 += 5
            col2 += 5
            col3 += 5

            # 取得した馬体重をeva_listにくっつける
            uma_bataijyu = df.iloc[0, col1]
            if uma_bataijyu is None:
                eva_list.at[i, "bataijyu"] = ""
                eva_list.at[i, "zogensa"] = ""
                return eva_list
            else:
                uma_zogenfugo = df.iloc[0, col2]
                uma_zogensa_tmp = df.iloc[0, col3]
                if uma_zogensa_tmp == "":
                    eva_list.at[i, "bataijyu"] = uma_bataijyu
                    eva_list.at[i, "zogensa"] = ""
                else:
                    uma_zogensa = str(uma_zogenfugo) + str(int(uma_zogensa_tmp))
                    # print(uma_zogensa)
                    eva_list.at[i, "bataijyu"] = uma_bataijyu
                    eva_list.at[i, "zogensa"] = uma_zogensa

        return eva_list

    except Exception as e:
        print("get_bataijyu() 当該レース馬体重取得処理で例外発生")
        conn.close()
        print(e)
        payload = {'message': e}
        requests.post(url, headers=headers, params=payload)
        return e

# 対戦型マイニング取得
def get_taisengata_mining(eva_list, row):
    year = row["Year"]
    monthday = row["MonthDay"]
    jyocd = row["JyoCD"]
    racenum = row["RaceNum"]

    try:
        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        # 抽出レース
        sql = "select * from s_taisengata_mining where Year = %(year)s and MonthDay = %(monthday)s " \
              " and JyoCD = %(jyocd)s and  RaceNum = %(racenum)s ; "
        # print(sql)
        # print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn,
                           params={"year": year, "monthday": monthday, 'jyocd': jyocd, "racenum": racenum})

        # print("sql実行終了")
        # print(df)
        db.close()

        col0 = 8
        col1 = 9

        if df.empty:
            eva_list.at[:, "t_mining"] = ""
            return eva_list

        for i, row3 in eva_list.iterrows():

            col0 += 2
            col1 += 2

            # 取得した対戦型マイニングをeva_listにくっつける
            uma_tmining_tmp = df.iloc[0, col1]
            if uma_tmining_tmp is None:
                eva_list.at[i, "t_mining"] = ""
                return eva_list
            else:
                uma_tmining = str(float(int(uma_tmining_tmp)/10))
                # print(uma_tmining)
                eva_list.at[i, "t_mining"] = uma_tmining

        return eva_list

        return df
    except Exception as e:
        print("get_taisengata_mining() 当該レース対戦型マイニング取得処理で例外発生")
        conn.close()
        print(e)
        payload = {'message': e}
        requests.post(url, headers=headers, params=payload)
        return e

# 馬評価テーブル登録（aggre_uma_evaluation）
def insert_uma_evaluation(row, uma_eva):

    try:
        # データ登録用DataRrame作成
        # （20200823）属性追加予定：オッズ、人気、馬体重、増減差、対戦型マインニング、マイニング人気、マイニング1位差
        upsert_data = pd.DataFrame({'Year': uma_eva['Year'], 'MonthDay': uma_eva['MonthDay'], 'JyoCD': uma_eva['JyoCD'],
                                    'RaceNum': uma_eva['RaceNum'], 'Umaban': uma_eva['Umaban'], 'Odds': uma_eva['Odds'],
                                    'Ninki': uma_eva['Ninki'], 'Bataijyu': uma_eva['bataijyu'],
                                    'Zogensa': uma_eva['zogensa'], 'Taisengata_mining':uma_eva['t_mining']
                                    })
        # print(upsert_data)
        # UPSERT処理
        tablename = "aggre_uma_evaluation"
        upsert_data.to_sql(tablename, conn2, if_exists='append', index=False,
                           index_label={'Year', 'MonthDay', 'JyoCD', 'RaceNum', 'Umaban'})

    except Exception as e:
        print("insert_uma_evaluation() 当該馬直前情報登録処理で例外発生")
        conn.close()
        print(e)
        payload = {'message': e}
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

# 汎用的傾向_合致結果抽出_値のみ（パーセント表記ではなく0.XX）
def get_match_factor_v(row2, race_trand_list, column_name, factor):

    column = row2[column_name]
    # print(column )

    row3 = race_trand_list[(race_trand_list["Factor"] == factor) & (race_trand_list["FactorValue"] == column)]
    # print(row3)

    # 値が空の場合は0.1を入れる（PowerBIで傾向を計算する際の仮値）
    if row3.empty:
        return_value = float(0.1)
    else:
        rate = row3["hukusyo_rate_2"].values[0]

        # print(rate)
        return_value = float(round(rate / 100, 3))
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

# ニックス傾向_合致結果抽出
def get_match_knicks_v(row2, race_trand_list, factor):

    # 種牡馬名とBMSを結合
    column = row2["Father"] + "×" + row2["Bms"]
    print(column)

    row3 = race_trand_list[(race_trand_list["Factor"] == factor) & (race_trand_list["FactorValue"] == column)]
    # print(row3)

    # 値が空の場合は0.1を入れる（PowerBIで傾向を計算する際の仮値）
    if row3.empty:
        return_value = float(0.1)
    else:
        rate = row3["hukusyo_rate_2"].values[0]

        # print(rate)
        return_value = float(round(rate / 100, 3))
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
    

    # テストデータ
    year = "2021"
    monthday = "0626"
    time = "0800"
    """

    # メイン処理を実行
    try:
        execute(year, monthday, time)
    except Exception as e:
        print("execute() 全体処理で例外発生")
        conn.close()
        print(e)
        payload = {'message': e}
        requests.post(url, headers=headers, params=payload)
        return e

# 実行開始
if __name__ == '__main__':
    main()
