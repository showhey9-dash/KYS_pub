#!usr/bin/env python
#
# JRDV提供の「体系コード」「特記コード」「走法コード」を変換する
#  各コードだけでは人間が見た時に理解できず、データ分析が出来ないため、
#  変換処理を作成する。
#  変換したデータは「jrdv_uma_taikei」テーブルにインサートする
#
# 【やりたい事】
#   ・各コース毎、馬場状態毎の成績と照らし合わせて得意不得意を評価するモデルを作成する
import sys

import pandas as pd
import pymysql
import numpy as np
from datetime import datetime, timedelta


# MySQLの接続情報（各自の環境にあわせて設定のこと）
db_config = {
    'host': 'localhost',
    'db': 'everydb',  # Database Name
    'user': 'root',
    'passwd': '******',
    'charset': 'utf8',
}

# 定数
# 特記コードのインデックス(関数readKyhFileで抽出したリストtaikeiListに対しての)
TAIKEI_SOUGOU_1 = 11
TAIKEI_SOUGOU_2 = 12
TAIKEI_SOUGOU_3 = 13
UMA_TOKKI_1 = 14
UMA_TOKKI_2 = 15
UMA_TOKKI_3 = 16


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


# 走法コード特定処理
def decideSouhou(i, souhouVal):
    # i は走法コードのインデックス
    # if文コメントの数値はJRDB発表のコード値（1始まり）

    souhou = "-"
    if i == 0:
        # 1 [走法]    全体的な走法        1:ピッチ,2:ストライド
        # print("idx:0")
        if souhouVal == "1":
            souhou = "ピッチ"
        elif souhouVal == "2":
            souhou = "ストライド"
        else:
            souhou = "-"
    elif i == 1:
        # 2 [脚使]    前脚の使い方        1:掻き込み,2:投げ出し／振り出し
        # print("idx:1")
        if souhouVal == "1":
            souhou = "掻き込み"
        elif souhouVal == "2":
            souhou = "投出/振出"
        else:
            souhou = "-"
    elif i == 2:
        # 3 [回転]    回転の速さ        1:速い,2:普通,3:遅い
        # print("idx:2")
        if souhouVal == "1":
            souhou = "速い"
        elif souhouVal == "2":
            souhou = "普通"
        elif souhouVal == "3":
            souhou = "遅い"
        else:
            souhou = "-"
    elif i == 3:
        # 4 [歩幅]    歩幅（完歩）の広さ    1:狭い,2:普通,3:広い
        # print("idx:3")
        if souhouVal == "1":
            souhou = "狭い"
        elif souhouVal == "2":
            souhou = "普通"
        elif souhouVal == "3":
            souhou = "広い"
        else:
            souhou = "-"
    elif i == 4:
        # 5 [脚上]    前脚の使い方        1:高い,2:普通,3:低い
        # print("idx:4")
        if souhouVal == "1":
            souhou = "高い"
        elif souhouVal == "2":
            souhou = "普通"
        elif souhouVal == "3":
            souhou = "低い"
        else:
            souhou = "-"
    else:
        # 6 予備
        # 7 予備
        # 8 予備
        # 設定なし
        # print("other")
        pass
    # print(souhou)
    return souhou


# 体系コード特定処理
def decideTaikei(i, taikeiVal):
    # i は体系コードのインデックス
    # if文コメントの数値はJRDB発表のコード値（1始まり）

    taikei = "-"
    if i == 0:
        # 1 [体型]    馬体の全体的な形状
        # 体型　（[体型]）
        # 1:長方形
        # 2:普通
        # 3:正方形
        # print("idx:" + str(i))
        if taikeiVal == "1":
            taikei = "長方形"
        elif taikeiVal == "2":
            taikei = "普通"
        elif taikeiVal == "3":
            taikei = "正方形"
        else:
            taikei = "-"
    elif i in {3, 5, 6, 8, 4, 9}:
        # 大きさ　（[4尻][6腹袋][7頭][9胸]） # 角度　（[5トモ][10肩]）
        # 1:大きい
        # 2:普通
        # 3:小さい
        # print("idx:" + str(i))
        if taikeiVal == "1":
            taikei = "大きい"
        elif taikeiVal == "2":
            taikei = "普通"
        elif taikeiVal == "3":
            taikei = "小さい"
        else:
            taikei = "-"
    elif i in {12, 13}:
        # 歩幅　（[13前幅][14後幅]）
        # 1:広い
        # 2:普通
        # 3:狭い
        # print("idx:" + str(i))
        if taikeiVal == "1":
            taikei = "広い"
        elif taikeiVal == "2":
            taikei = "普通"
        elif taikeiVal == "3":
            taikei = "狭い"
        else:
            taikei = "-"
    elif i in {7, 2, 1, 10, 11, 14, 15}:
        # 長さ　（[8首][3胴][2背中][11前肢][12後肢][15前繋][16後繋]）
        # 1:長い
        # 2:普通
        # 3:短い
        # print("idx:" + str(i))
        if taikeiVal == "1":
            taikei = "長い"
        elif taikeiVal == "2":
            taikei = "普通"
        elif taikeiVal == "3":
            taikei = "短い"
        else:
            taikei = "-"
    elif i == 16:
        # 17 [尾]        つけ根の上げ方    1:上げる,2:下げる
        # print("idx:" + str(i))
        if taikeiVal == "1":
            taikei = "上げる"
        elif taikeiVal == "2":
            taikei = "下げる"
        else:
            taikei = "-"
    elif i == 17:
        # 18 [振]        尾の振り方    1:激しい,2:少し,3:あまり振らない
        # print("idx:" + str(i))
        if taikeiVal == "1":
            taikei = "激しい"
        elif taikeiVal == "2":
            taikei = "少し"
        elif taikeiVal == "3":
            taikei = "あまり振らない"
        else:
            taikei = "-"
    else:
        # 19 予備
        # 20 予備
        # 21 予備
        # 22 予備
        # 23 予備
        # 24 予備
        # 25 予備
        # 26 予備
        # print("other")
        pass
    # print("idx:" + str(i) + ",体系コード:" + taikeiVal + ",体系:" +taikei)
    return taikei


# 特記コード特定処理
def decideTokki(tokkiVal):
    # ※特記コードが文字列だとうまく行かないので呼び出し元にintに変換してもらう
    tokkiData = '-'

    # print(tokkiVal)
    if tokkiVal == 0:
        return tokkiData

    # 特記コードのcsvを読み込み
    filePath = "../csv/tokkiCode_20181130.csv"
    fileDf = pd.read_csv(filePath, engine='python', encoding='cp932', index_col='tokkiCode')
    # print(fileDf)

    # 読み込んだcsvの列1にある特記コードから 引数の特記コードを検索
    # tokkiData = fileDf.query('tokkiCode == @tokkiVal' )
    try:
        tokkiData = fileDf.at[tokkiVal, 'TokkiRyakusyo']
    except:
        print("例外発生：tokkiData = fileDf.at[tokkiVal,'TokkiRyakusyo']")
        tokkiData = '-'
    # print(type(tokkiData))
    # print(tokkiData)

    # ※ queryを使用した場合に必要
    # 取得した行の略称（2列目）を返却
    # tokkiRyakusyo = tokkiData['TokkiRyakusyo']
    # print(type(tokkiRyakusyo))
    # print(tokkiRyakusyo[0])

    # print("特記コード:" + tokkiVal + ",特記:" + tokkiRyakusyo)
    return tokkiData.replace("　", "")


# ファイル読み込み（KYH）と格納データ抽出、変換
def readKyhFile(yyMMdd):
    try:
        # 対象ファイルを特定
        fileDir = 'F:\showhey\jrdbcometemp\\'

        # 競走馬データファイル名
        kyhFileName = "KYH" + yyMMdd + ".txt"
        # ファイルパス
        filePath = fileDir + kyhFileName
        # print(filePath)

        taikeiList = []

        # ファイル読み込み、ファイル内からレースＩＤ、血統番号、馬名、コードを抽出しリストに格納
        # with句はファイル閉じるを兼ねる
        with open(filePath, 'r') as f1:

            for i, inLine in enumerate(f1):
                # print(inLine)
                # データ取得
                jyoCod = inLine[0:2]  # 場コード
                year = inLine[2:4]  # 年
                kaiji = inLine[4:5]  # 回次
                nichiji = inLine[5:6]  # 日次
                raceNum = inLine[6:8]  # レース番号
                umaban = inLine[8:10]  # 馬番
                kettoNum = inLine[10:18]  # 血統番号
                bamei = inLine[18:36]  # 馬名　※怪しい
                souhouCode = inLine[417:425]  # 走法コード
                taikeiCode = inLine[425:449]  # 体系コード
                taikeiSougou1Code = inLine[449:452]  # 体系総合コード１
                taikeiSougou2Code = inLine[452:455]  # 体系総合コード２
                taikeiSougou3Code = inLine[455:458]  # 体系総合コード３
                tokki1Code = inLine[458:461]  # 特記１コード
                tokki2Code = inLine[461:464]  # 特記２コード
                tokki3Code = inLine[464:467]  # 特記３コード

                # yearとkettonumの頭に20を追加() →　対応止め
                # year = '20' + year
                # kettoNum = '20' + kettoNum

                # 取得データの置換（名称の全角スペース→空文字、コードの半角スペース→0）
                bamei = bamei.replace("　", "")
                souhouCode = souhouCode.replace(" ", "0")
                taikeiCode = taikeiCode.replace(" ", "0")
                taikeiSougou1Code = taikeiSougou1Code.replace(" ", "0")
                taikeiSougou2Code = taikeiSougou2Code.replace(" ", "0")
                taikeiSougou3Code = taikeiSougou3Code.replace(" ", "0")
                tokki1Code = tokki1Code.replace(" ", "0")
                tokki2Code = tokki2Code.replace(" ", "0")
                tokki3Code = tokki3Code.replace(" ", "0")

                # 年月日を追加
                nengappi = '20' + yyMMdd

                inList = [jyoCod, year, kaiji, nichiji, raceNum, umaban, kettoNum, nengappi, bamei, souhouCode,
                          taikeiCode,
                          taikeiSougou1Code, taikeiSougou2Code, taikeiSougou3Code, tokki1Code, tokki2Code, tokki3Code, ]
                taikeiList.append(inList)
                # if i==10:
                #   break
        # print(taikeiList)
        return taikeiList
    except FileNotFoundError:
        print("FileNotFoundError:ファイルが存在しない")
        return taikeiList


# from datetime import datetime
# conn = pymysql.connect(host='localhost', user='root', password='systemsss', db='everydb')
conn = pymysql.connect(host=db_config['host'], user=db_config['user'],
                       password=db_config['passwd'], db=db_config['db'])


# jrdv_uma_taikeiにinsert
def insertJrdvUmaTaikei(taikeiList):
    # startTime = datetime.now()
    # print("start:insertJrdvUmaTaikei" + str(startTime))
    # カーソル取得
    # cur = conn.cursor(buffered=True)
    cur = conn.cursor()
    # %sの数は動確のためreadKyhFileで抽出する項目数と一致させる(=16)、全量は54
    # sql ='INSERT INTO jrdv_uma_taikei (`jyoCd`,`Year`,`Kaiji`,`Nichiji`,`RaceNum`,`Umaban`,`KettoNum`,`Bamei`,`SouhouCode`,`TaikeiCode`,`TaikeiSougou1Code`,`TaikeiSougou2Code`,`TaikeiSougou3Code`,`UmaTokki1Code`,`UmaTokki2Code`,`UmaTokki3Code`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    sql = 'INSERT INTO jrdv_uma_taikei VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
          '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

    param = []
    # print("start:登録データ作成" + str(datetime.now()))
    for data in taikeiList:
        param.append([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                      data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
                      data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23],
                      data[24], data[25], data[26], data[27], data[28], data[29], data[30], data[31],
                      data[32], data[33], data[34], data[35], data[36], data[37], data[38], data[39],
                      data[40], data[41], data[42], data[43], data[44], data[45], data[46], data[47],
                      data[48], data[49], data[50], data[51], data[52], data[53], data[54]
                      ])
    # print("end:登録データ作成" + str(datetime.now()))
    # リスト分実行
    # SQL作成、発行（insertmany関数使えるか？）
    cur.executemany(sql, param)
    conn.commit()


# 走法コード和名取得、リストに追加して変換
def addSouhouMeisyo(taikeiList):
    # 走行コードのみを抽出するにndarrayに変換
    taikeiList2 = np.array(taikeiList)

    # 走法コードのindexは「8」
    souhouCodeList = taikeiList2[:, 9]
    # souhouCode = taikeiList[8]
    # print(type(souhouCodeList))
    # souhouCodeStr = souhouCode.astype('str')
    # print(type(souhouCodeStr))
    for i, souhouCode in enumerate(souhouCodeList):
        # print("i:" + str(i) + ",souhouCode:" + souhouCode)
        for j, souhouVal in enumerate(souhouCode):
            # print(souhouVal)
            # 体系リストの末尾に取得した走法名を追加していく
            val = decideSouhou(j, souhouVal)
            taikeiList[i].extend(val)
    return taikeiList


# 体系コード和名取得、リストに追加して変換
def addTaikeiMeisyo(taikeiList):
    # 体系コードのみを抽出するにndarrayに変換
    taikeiList2 = np.array(taikeiList)

    # 体系コードのindexは「9」
    taikeiCodeList = taikeiList2[:, 10]
    # print(type(taikeiCodeList))
    for i, taikeiCode in enumerate(taikeiCodeList):
        # print("i:" + str(i) + ",taikeiCode:" + taikeiCode)
        for j, taikeiVal in enumerate(taikeiCode):
            # print(taikeiVal)
            # 体系リストの末尾に取得した走法名を追加していく
            val = decideTaikei(j, taikeiVal)
            # print("val:" + val)
            taikeiList[i].append(val)
    return taikeiList


# 特記コード和名取得、リストに追加して変換
def addTokkiMeisyo(taikeiList, tokkiType):
    # ダミー
    # tokkiType = 10

    # 特記コードのみを抽出するにndarrayに変換
    taikeiList2 = np.array(taikeiList)

    # 特記コードは総合、特記ともに１～３あるので呼び出し元にもらう必要がある
    taikeiCodeList = taikeiList2[:, tokkiType]
    for i, tokkiCode in enumerate(taikeiCodeList):
        # print("i:" + str(i) + ",tokkiCode:" + tokkiCode)
        # 体系リストの末尾に取得した特記名を追加していく
        val = decideTokki(int(tokkiCode))
        # print("i:" + str(i) + ",tokkiCode:" + tokkiCode + ",特記:" + val)
        taikeiList[i].append(val)
    return taikeiList


# メイン処理
def execute(fromDateStr, toDateStr):
    startTime = datetime.now()
    print("start:insertJrdvUmaTaikei" + str(startTime))

    # 引数をdate型に
    fromDate = datetime(int(fromDateStr[0:4]), int(fromDateStr[4:6]), int(fromDateStr[6:8]))
    toDate = datetime(int(toDateStr[0:4]), int(toDateStr[4:6]), int(toDateStr[6:8]))

    # カレンダーとしてのカウンター
    delta = timedelta(days=1)

    # 体系コード変換、DB登録処理
    while fromDate <= toDate:
        # print(fromDate.strftime("%Y%m%d"))
        kyhDate = fromDate.strftime("%Y%m%d")[2:8]
        print(kyhDate)
        # csv ファイル読み込み（KYH）と格納データ抽出、変換
        taikeiList = readKyhFile(kyhDate)

        if (len(taikeiList) == 0):
            fromDate += delta
            continue

        # 走法コード,体系コード,特記コードの和名を順次取得
        taikeiList = addSouhouMeisyo(taikeiList)
        taikeiList = addTaikeiMeisyo(taikeiList)
        taikeiList = addTokkiMeisyo(taikeiList, TAIKEI_SOUGOU_1)
        taikeiList = addTokkiMeisyo(taikeiList, TAIKEI_SOUGOU_2)
        taikeiList = addTokkiMeisyo(taikeiList, TAIKEI_SOUGOU_3)
        taikeiList = addTokkiMeisyo(taikeiList, UMA_TOKKI_1)
        taikeiList = addTokkiMeisyo(taikeiList, UMA_TOKKI_2)
        taikeiList = addTokkiMeisyo(taikeiList, UMA_TOKKI_3)

        # print(taikeiList)

        # DB登録
        insertJrdvUmaTaikei(taikeiList)
        fromDate += delta

    endTime = datetime.now()
    print("end:insertJrdvUmaTaikei" + str(endTime))
    print("処理時間" + str(endTime - startTime))


def main():
    # 引数をコマンドラインから入力
    year = input("Year?: ")
    startDay = input("StartDay?: ")
    endDay = input("EndDay?: ")

    if not inputChk(year, startDay, endDay):
        print('---end---')
        sys.exit()

    # メイン処理を実行
    execute(year + startDay, year + endDay)


if __name__ == '__main__':
    main()


