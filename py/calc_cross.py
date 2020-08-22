#!usr/bin/env python
#
# 5代血統表クロス集計
#
# 競走馬の5代血統表をDBより取得し、総当たりでクロスを計算する
# 父を起点に１とし、母母母母母まで述べ６２頭が登場する
# 奇数が父、偶数が母となる
# 1代目：50%、2代目：25%、3代目：12.5%、4代目：6.25%、5代目3.125%とし、
# 合致した代で加算していく
# インプット：
# TFの馬コメントにクロス情報を記入する

# import
import sys

import MySQLdb
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

def execute(year):

    print("処理開始")

    # 入力された年を基に対象リストを取得
    blood_list =  get_5th_blood_list(year)

    # blood_listから不要なカラムを削除する

    # 取得した対象馬リストそれぞれのクロス計算を行う
    calc_cross(year, blood_list)


# 5代血統表を取得する
def get_5th_blood_list(year):

    # カーソル取得
    db = conn.cursor(MySQLdb.cursors.DictCursor)

    filePath = "../sql/5th_blood.sql"
    targetSqlFile = open(filePath)
    sql = targetSqlFile.read()  # 終端まで読み込んだdataを返却
    # print(sql)

    year2 = year + "%"

    # PandasのDataFrameの型に合わせる方法
    df = psql.read_sql(sql, conn, params={"year": year2}, )
    # print(df)

    print("sql実行終了")
    # print(df)
    return df

# 5代血統表を基にクロスを計算
def calc_cross(year, df):

    # dataframeを1行取得する
    for i,row in df.iterrows():

        print(row['Bamei'])

        # 血量計算済みリスト
        counted_list = []
        # クロスリスト
        cross_list = []

        # d1～d62カラムの比較元の取得
        for idx, pbamei in enumerate(row[6::],1):
            # print(idx)
            # print(type(pbamei))
            # print(pbamei)

            if pbamei is None:
                continue

            if pbamei in counted_list:
                # print("★★★")
                continue

            # pbamei 血用計算済みフラグ
            pbameiFlg = False
            # 血量
            blood_vol = 0

            idx2 = 6 + idx
            # 比較対象を取得
            for idx3, pbamei2 in enumerate(row[idx2::], idx + 1):
                # print( str(idx3) + " 比較対象" + pbamei2)

                if pbamei == pbamei2:
                    #idx3 = idx2 - 6
                    # print("一致！！！" + ":idx3 " + str(idx3))


                    # 血量を計算する
                    if not pbameiFlg:

                        # 比較元の血量を算出する

                        # pbameiの血量を加算する
                        blood_vol = blood_volume(idx)
                        pbameiFlg = True
                    # 比較対象の血量を算出する
                    blood_vol = blood_vol + blood_volume(idx3)

                # 計算した馬名をリスト保持（重複防止）
                counted_list.append(pbamei)

            if blood_vol > 0:
                # print(type(blood_vol))
                # print(blood_vol)
                if idx % 2 == 0:
                    cross_word = "母" + pbamei + " : " + str(blood_vol) + "%"
                else:
                    cross_word = pbamei + " : " + str(blood_vol) + "%"
                cross_list.append(cross_word)

        print(cross_list)
        #print(blood_vol)

        # TargetFrontierの馬コメントにクロス情報を書き込む
        write_tf_uma_comment(year, row['KettoNum'], cross_list)

        # 開発用　10件で終了
        #if i == 3:
        #    break

    print(i)
    print("処理終了")

# TargetFrotierにクロス情報を書き込む
def write_tf_uma_comment(year, kettonum, cross_list):

    #print("書き込み開始")

    # 対象ファイルを特定
    fileDir = r'C:\TFJV\MY_DATA\UMA_COM\\' + year

    # 血統番号末尾1桁を基で対象ファイルを選定
    kettonum_l = kettonum[9]
    # print(kettonum_l)
    file_name = "UC" + year + kettonum_l + ".DAT"

    # ファイルパス
    file_path = fileDir + '\\' + file_name

    # 対象ファイルを追記モードで開く

    # 文言作成
    # 血統番号, クロス情報
    cross_list_mojiretsu = '_'.join(cross_list)

    new_line = kettonum + "," + cross_list_mojiretsu + "\r\n"

    # ファイルに書き込み
    with open(file_path, mode='a') as f:
        f.write(new_line)

    # ファイルを閉じる

    #print("書き込み終了")

# 血量を算出して返却する
# 1代目：50%、2代目：25%、3代目：12.5%、4代目：6.25%、5代目3.125%
# idx2以下：50　3～6：25　7～14：12.5　15～30：6.25　31～62：3.125
def blood_volume(idx):

    if idx <= 2:
        return  50
    elif idx <= 6:
        return  25
    elif idx <= 14:
        return 12.5
    elif idx <= 30:
        return 6.25
    elif idx <= 62:
        return 3.125
    else:
        return 0

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
    year = input("Year?: ")

    if not inputChk(year):
        print('---end---')
        sys.exit()

    # メイン処理を実行
    execute(year)


if __name__ == '__main__':
    main()