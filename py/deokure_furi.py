#!user/bin/env python
# １．出遅れ解消により前走以上の好走が期待できる馬を、今回も出遅れそうな馬、要注意馬を抽出
# Targetの予想コメントの末尾に「出遅れ解消」と追記
# ２．前走不利を受け、力を発揮できず、巻き返しが期待できる馬を抽出

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


# MySQLの接続情報（各自の環境にあわせて設定のこと）
db_config = {
    'host': 'localhost',
    'db': 'everydb',  # Database Name
    'user': 'root',
    'passwd': '*****',
    'charset': 'utf8',
}


# conn = MySQLdb.connect(host=db_config['host'], db=db_config['db'], user=db_config['user'], passwd=db_config['passwd'], charset=db_config['charset'])
conn = mysqlCon.connect(host='localhost', user='root', passwd='systemsss', db='everydb')

# Targetの予想コメント末尾に「　モデルＸ推奨馬」と記載
def writeYosouComment(modelType, raceId):
    year = "2020"
    # 対象ファイルを特定
    fileDir = 'C:\TFJV\MY_DATA\YOS_COM\\' + year

    # raceIdをstrに
    raceIdStr = str(raceId)
    raceIds = raceIdStr[0:6]
    # print(raceIds)

    # 予想コメントファイル名
    ycFileName = "YC" + raceIds + ".dat"
    # ファイルパス
    filePath = fileDir + '\\' + ycFileName
    # print(filePath)

    newLine = ''
    lineIndex = 0

    # ファイル読み込み、ファイル内からレースＩＤを検索
    # with句はファイル閉じるを兼ねる
    with open(filePath, 'r') as f1:

        for i, inLine in enumerate(f1):
            # 更新対象行を特定
            # あれば、文言の更新
            if raceId in inLine:
                # print(str(i) + ":" + inLine)
                lineIndex = i
                newLine = inLine.rstrip('\n') + modelType + "\n"
                break

    # 更新用データの読み込み
    with open(filePath) as f2:
        l = f2.readlines()

    # 更新情報の挿入と、不要行の削除
    # print(lineIndex)
    # print(newLine)
    l.insert(lineIndex, newLine)
    del l[lineIndex + 1]

    # ファイルへの書き込み
    with open(filePath, 'w') as f3:
        f3.writelines(l)


# DBから当日出走馬リストを取得 引数 年(yyyy)、月日(MMdd)
def getSyussouList(year, monthDay):
    # print(conn)

    # カーソル取得
    db = conn.cursor(buffered=True)

    # 血統番号（頭2桁はトリム）
    sql = "select SUBSTRING(KettoNum,3),concat(JyoCD,substring(Year,3),substring(Kaiji,2), " \
          "substring(Nichiji,2),RACENUM,Umaban) , nur.Bamei from n_uma_race nur " \
          "where nur.year = %s and nur.monthday = %s and nur.JyoCD <= '10' order by JyoCD, RaceNum, Umaban"
    db.execute(sql, [year, monthDay])

    # 表示
    rows = db.fetchall()
    # print(rows)
    return rows


# その競走馬の過去N走を取得
# 検索キー'KettoNum' ソート降順
def getSyussouRireki(n, kettoNum, nengappi):
    # print(conn)
    # カーソル取得
    db = conn.cursor(buffered=True)

    sql = "select * from jrdv_uma_race  jur where jur.KettoNum in (%s) and jur.Nengappi < %s " \
          " order by jur.Nengappi desc limit %s"
    db.execute(sql, [kettoNum, nengappi, n])

    # 表示
    rows = db.fetchall()
    # print(rows)
    return rows


# 出遅れ評価
# 評価：２走連続出遅れ→「出遅れ消」、前走、3走前で出遅れ「出遅注意」、2走前と3走前で出遅れ「出遅解消！」、近３走で前走のみ出遅れ「前走出遅」
import re


def deokureEvaluation(syussouRireki):
    # print(len(syussouRireki))

    # 出走履歴３走未満はチェックしない
    if len(syussouRireki) < 3:
        return ''

    # 取得リストの中の出遅れ位置
    deokureIdx = 34
    sou1Deokure = syussouRireki[0][deokureIdx]
    sou2Deokure = syussouRireki[1][deokureIdx]
    sou3Deokure = syussouRireki[2][deokureIdx]

    # print ('前走:' + sou1Deokure + ' 2走前:' + sou2Deokure + ' 3走前' + sou3Deokure)
    # print(type(sou1Deokure))

    # sou1~3Deokureから出遅れ情報を抽出
    sou1Obj = re.search(r'[1-9]', sou1Deokure)
    sou2Obj = re.search(r'[1-9]', sou2Deokure)
    sou3Obj = re.search(r'[1-9]', sou3Deokure)

    deokureEva = ''

    if sou1Obj and sou2Obj:
        # print('出遅消し！')
        deokureEva = ' 出遅消し！'
    elif sou1Obj and sou3Obj:
        # print('出遅注意')
        deokureEva = ' 出遅注意'
    elif sou1Obj:
        # print('前走出遅')
        deokureEva = ' 前走出遅'
    elif sou2Obj and sou3Obj:
        # print('出遅解消！')
        deokureEva = ' 出遅解消！'
    else:
        # print('出遅なし')
        deokureEva = ''
    return deokureEva


# 前走不利評価
# 前走不利があった場合、「不利○馬身」
def checkZensouFuri(syussouRireki):
    # print(len(syussouRireki))

    # 出走履歴1走未満はチェックしない
    if len(syussouRireki) < 1:
        return ''

    # 取得リストの中の不利位置
    furiIdx = 36
    sou1Furi = syussouRireki[0][furiIdx]
    # print(sou1Furi)

    # sou1Furiから出遅れ情報を抽出
    sou1Obj = re.search(r'[1-9]', sou1Furi)
    # print(type(sou1Obj))

    furiStr = ''

    if sou1Obj:
        # print(sou1Obj)
        furiStr = ' 前走不利' + sou1Obj.group() + '馬身'
        return furiStr
    else:
        return furiStr


# 入力チェック
def inputChk(year, startDay, endDay):
    print('start--inputChk')
    # 空文字チェック
    if year == '' or startDay == '' or endDay == '':
        print("入力値に空文字があります")
        return False
    # 対象文字以外
    elif not year.isdigit() or not startDay.isdigit() or not endDay.isdigit():
        print("入力値に数値以外があります")
        return False
    # 4桁チェック
    elif len(year) != 4 or len(startDay) != 4 or len(endDay) != 4:
        print("入力値の桁数が不正です")
        return False
    # 日付の前後チェック
    elif startDay > endDay:
        print("月日の大小が不正です")
        return False
    else:
        return True


# 該当馬を出力（TFへ書き込みetc）
# 該当馬のみ抽出
def output(data3, modeltype,  nengappi):
    print('output--start')

    """
    raceIdList = pd.DataFrame()
    for umaData in data3.iterrows():
        # print(type(umaData))
        
        umaDataCol = umaData[1]
        # print(umaDataCol['key'])
        if umaDataCol['key'] == 1:
            print(umaDataCol['jyoCd'], umaDataCol['RaceNum'], umaDataCol['Umaban'], umaDataCol['Bamei'])
            # Targetの予想コメントの末尾に「　モデルＸ推奨馬」と記載したい
            raceId = umaDataCol['jyoCd'] + umaDataCol['Year'] + umaDataCol['Kaiji'] + umaDataCol['Nichiji'] + umaDataCol['RaceNum'] + umaDataCol['Umaban']
            print(raceId)
            #print(type(raceId))
            writeYosouComment(" 体系モデルA推奨馬", raceId)
            raceId_index = pd.DataFrame([[umaDataCol['jyoCd'], umaDataCol['Year'], umaDataCol['Kaiji'], umaDataCol['Nichiji'], umaDataCol['RaceNum'], umaDataCol['Umaban'], umaDataCol['Bamei']]], columns=['jyoCd','Year','Kaiji','Nichiji','RaceNum','Umaban','Bamei'] )
            #print(raceId_index)
            raceIdList = pd.concat([raceIdList, raceId_index])
    """
    # csv出力
    path = "../output/csv/" + modeltype + "/" + nengappi + '.csv'
    data3.to_csv(path)

# メインメソッドの流れ
# import
def execute(fromDateStr, toDateStr):

    # 対象モデル名
    model_type = 'taikeiA'

    startTime = datetime.now()
    print("start:aggre_model_taikeiA" + str(startTime))

    # 引数をdate型に
    fromDate = datetime(int(fromDateStr[0:4]), int(fromDateStr[4:6]), int(fromDateStr[6:8]))
    toDate = datetime(int(toDateStr[0:4]), int(toDateStr[4:6]), int(toDateStr[6:8]))

    # カレンダーとしてのカウンター
    delta = timedelta(days=1)

    # 番組情報取得、DB登録処理
    while fromDate <= toDate:
        # print(fromDate.strftime("%Y%m%d"))
        bacDate = fromDate.strftime("%Y%m%d")[:8]
        print(bacDate)

        # TODO 本処理
        # DBから当日出走馬リストを取得
        syussouList = getSyussouList(bacDate[0:4], bacDate[4:8])
        # print(type(syussouList))

        deokure_keshi_list = pd.DataFrame()
        deokure_tyui_list = pd.DataFrame()
        zensou_deokure_list = pd.DataFrame()
        deokure_kaisyo_list = pd.DataFrame()
        zensou_furi_list = pd.DataFrame()

        # 以下、繰り返し
        for syussouBa in syussouList:
            print(syussouBa)
            # 各馬ごとの近3走のデータを取得
            syussouRireki = getSyussouRireki(3, syussouBa[0], bacDate)
            # 出遅れ状況を評価
            deokureEva = deokureEvaluation(syussouRireki)

            # if deokureEva != '':
                # print(deokureEva)
                # Targetの予想コメントの末尾に評価を記載
                # writeYosouComment(deokureEva, syussouBa[1])

                # 集計用csvデータセットに追加
            if deokureEva == ' 出遅消し！':

                raceId_index = pd.DataFrame([[syussouBa[1][0:2], syussouBa[1][2:4], syussouBa[1][4:5],
                                              syussouBa[1][5:6], syussouBa[1][6:8], syussouBa[1][8:10],
                                              syussouBa[2]]],
                                            columns=['jyoCd', 'Year', 'Kaiji', 'Nichiji', 'RaceNum', 'Umaban', 'Bamei'])
                
                print(raceId_index)
                deokure_keshi_list = pd.concat([deokure_keshi_list, raceId_index])

            elif deokureEva == ' 出遅注意':
                raceId_index = pd.DataFrame([[syussouBa[1][0:2], syussouBa[1][2:4], syussouBa[1][4:5],
                                              syussouBa[1][5:6], syussouBa[1][6:8], syussouBa[1][8:10],
                                              syussouBa[2]]],
                                            columns=['jyoCd', 'Year', 'Kaiji', 'Nichiji', 'RaceNum', 'Umaban', 'Bamei'])

                print(raceId_index)
                deokure_tyui_list = pd.concat([deokure_tyui_list, raceId_index])
            elif deokureEva == ' 前走出遅':
                raceId_index = pd.DataFrame([[syussouBa[1][0:2], syussouBa[1][2:4], syussouBa[1][4:5],
                                              syussouBa[1][5:6], syussouBa[1][6:8], syussouBa[1][8:10],
                                              syussouBa[2]]],
                                            columns=['jyoCd', 'Year', 'Kaiji', 'Nichiji', 'RaceNum', 'Umaban', 'Bamei'])

                print(raceId_index)
                zensou_deokure_list = pd.concat([zensou_deokure_list, raceId_index])
            elif deokureEva == ' 出遅解消！':
                raceId_index = pd.DataFrame([[syussouBa[1][0:2], syussouBa[1][2:4], syussouBa[1][4:5],
                                              syussouBa[1][5:6], syussouBa[1][6:8], syussouBa[1][8:10],
                                              syussouBa[2]]],
                                            columns=['jyoCd', 'Year', 'Kaiji', 'Nichiji', 'RaceNum', 'Umaban', 'Bamei'])

                print(raceId_index)
                deokure_kaisyo_list = pd.concat([deokure_kaisyo_list, raceId_index])
            else:
                pass

            # 前走不利を評価
            checkFuri = checkZensouFuri(syussouRireki)

            # Targetの予想コメントの末尾に評価を記載
            if checkFuri != '':
                print(checkFuri)
                # writeYosouComment(checkFuri, syussouBa[1])

                # 集計用csvデータセットに追加
                raceId_index = pd.DataFrame([[syussouBa[1][0:2], syussouBa[1][2:4], syussouBa[1][4:5],
                                              syussouBa[1][5:6], syussouBa[1][6:8], syussouBa[1][8:10],
                                              syussouBa[2]]],
                                            columns=['jyoCd', 'Year', 'Kaiji', 'Nichiji', 'RaceNum', 'Umaban', 'Bamei'])

                print(raceId_index)
                zensou_furi_list = pd.concat([zensou_furi_list, raceId_index])

        # output
        output(deokure_keshi_list, "deokure_keshi",  bacDate)
        output(deokure_tyui_list, "deokure_tyui", bacDate)
        output(zensou_deokure_list, "zensou_deokure", bacDate)
        output(deokure_kaisyo_list, "deokure_kaisyo", bacDate)
        output(zensou_furi_list, "zensou_furi", bacDate)

        # 繰り返し、ここまで
        print("処理終了")
        # 処理終了


        fromDate += delta

    endTime = datetime.now()
    print("end:aggre_model_taikeiA" + str(endTime))
    print("処理時間" + str(endTime - startTime))


def main():
    #引数をコマンドラインから入力
    year = input("Year?: ")
    startDay = input("StartDay?: ")
    endDay = input("EndDay?: ")

    if not inputChk(year, startDay, endDay):
        print('---end---')
        sys.exit()

    #メイン処理を実行
    execute(year + startDay, year + endDay)


if __name__ == '__main__':
    main()
