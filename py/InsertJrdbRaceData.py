#!usr/bin/env python

# JRDV提供の「番組データ仕様」をMySQLに登録する
#  "BAC" + 年月日 + ".txt"から一部データを抽出し
#  「jrdv_race_data」テーブルにインサートする

import unicodedata
import pymysql
# import datetime
from datetime import datetime, timedelta


# MySQLの接続情報（各自の環境にあわせて設定のこと）
db_config = {
    'host': 'localhost',
    'db': 'everydb',  # Database Name
    'user': 'root',
    'passwd': '*****',
    'charset': 'utf8',
}

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

# 半角を1文字、全角2を文字としてカウントするコード(note.nkmk.me)
def get_east_asian_width_count(text):
    count = 0
    for c in text:
        if unicodedata.east_asian_width(c) in 'FWA':
            count += 2
        else:
            count += 1
    return count


# ファイル読み込み（Bac）と格納データ抽出、変換
def readBacFile(yyMMdd):

    # print('start--readBacFile')

    try:
        # 対象ファイルを特定
        fileDir = 'F:\showhey\jrdbcometemp\\'

        # 競走馬データファイル名
        bacFileName = "BAC" + yyMMdd + ".txt"
        # ファイルパス
        filePath = fileDir + bacFileName
        # print(filePath)

        bangumiList = []

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
                nengappi = inLine[8:16]  # 年月日
                hassoujikan = inLine[16:20]  # 発走時間
                kyori = inLine[20:24]  # 距離
                shibaDirt = inLine[24:25]  # 芝ダート
                LR = inLine[25:26]  # 左右周り
                uchisoto = inLine[26:27]  # 内外
                syubetsu = inLine[27:29]  # 種別
                jyoken = inLine[29:31]  # 条件
                kigo = inLine[31:34]  # 記号
                jyuryo = inLine[34:35]  # 重量
                grade = inLine[35:36]  # グレード

                raceName = inLine[36:76]  # レース名  ※全角半角混在
                overRN = get_east_asian_width_count(raceName) - 40  # 全角によるオーバー文字数を取得
                # print(overRN)
                raceName = inLine[36:86 - overRN]  # オーバー文字数を減産
                kaisu = inLine[86 - overRN:94 - overRN]  # 回数     ※全角半角混在
                overKaisu = get_east_asian_width_count(kaisu) - 8  # 全角によるオーバー文字数を取得
                # print(overKaisu)
                kaisu = inLine[86 - overRN:94 - (overRN + overKaisu)]  # オーバー文字数を減産
                tousu = inLine[94 - (overRN + overKaisu):96 - (overRN + overKaisu)]  # 頭数
                course = inLine[96 - (overRN + overKaisu):97 - (overRN + overKaisu)]  # コース
                dataKubun = inLine[124 - (overRN + overKaisu):125 - (overRN + overKaisu)]  # データ区分

                # yearとkettonumの頭に20を追加() →　対応止め
                # year = '20' + year
                # kettoNum = '20' + kettoNum

                # 取得データの置換（名称の全角スペース→空文字、コードの半角スペース→0）
                raceName = raceName.replace("　", "")
                kaisu = kaisu.replace("　", "")
                course = course.replace(" ", "0")

                # 年月日を追加
                nengappi = '20' + yyMMdd

                inList = [jyoCod, year, kaiji, nichiji, raceNum, nengappi, hassoujikan, kyori, shibaDirt,
                          LR, uchisoto, syubetsu, jyoken, kigo, jyuryo, grade, raceName, kaisu, tousu, course,
                          dataKubun]
                bangumiList.append(inList)
                # if i==10:
                #   break
        # print(bangumiList)

        # print('end--readBacFile')
        return bangumiList
    except FileNotFoundError:
        print("FileNotFoundError:ファイルが存在しない")
        return bangumiList

# from datetime import datetime
conn = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['passwd'], db=db_config['db'])

# jrdv_race_Bangumiにinsert
def insertJrdvRaceBangumi(bangumiList):

    print('start--insertJrdvRaceBangumi')

    # startTime = datetime.now()
    # print("start:insertJrdvRaceBangumi" + str(startTime))
    # カーソル取得
    # cur = conn.cursor(buffered=True)
    cur = conn.cursor()
    # %sの数は動確のためreadKyhFileで抽出する項目数と一致させる(=16)、全量は54
    # sql ='INSERT INTO jrdv_uma_taikei (`jyoCd`,`Year`,`Kaiji`,`Nichiji`,`RaceNum`,`Umaban`,`KettoNum`,`Bamei`,`SouhouCode`,`TaikeiCode`,`TaikeiSougou1Code`,`TaikeiSougou2Code`,`TaikeiSougou3Code`,`UmaTokki1Code`,`UmaTokki2Code`,`UmaTokki3Code`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    sql = 'INSERT INTO jrdv_race_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

    param = []
    # print("start:登録データ作成" + str(datetime.now()))
    for data in bangumiList:
        param.append([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                      data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
                      data[16], data[17], data[18], data[19], data[20],
                      ])
    # print("end:登録データ作成" + str(datetime.now()))
    # リスト分実行
    # SQL作成、発行（insertmany関数使えるか？）
    cur.executemany(sql, param)
    conn.commit()


# メイン処理
def execute(fromDateStr, toDateStr):
    startTime = datetime.now()
    print("start:insertJrdvRaceBangumi" + str(startTime))

    # 引数をdate型に
    fromDate = datetime(int(fromDateStr[0:4]), int(fromDateStr[4:6]), int(fromDateStr[6:8]))
    toDate = datetime(int(toDateStr[0:4]), int(toDateStr[4:6]), int(toDateStr[6:8]))

    # カレンダーとしてのカウンター
    delta = timedelta(days=1)

    # 番組情報取得、DB登録処理
    while fromDate <= toDate:
        # print(fromDate.strftime("%Y%m%d"))
        bacDate = fromDate.strftime("%Y%m%d")[2:8]
        print(bacDate)
        # csv ファイル読み込み（BAC）
        bangumiList = readBacFile(bacDate)

        if (len(bangumiList) == 0):
            fromDate += delta
            continue

        # DB登録
        insertJrdvRaceBangumi(bangumiList)
        fromDate += delta

    endTime = datetime.now()
    print("end:insertJrdvRaceBangumi" + str(endTime))
    print("処理時間" + str(endTime - startTime))


def main():
    #引数をコマンドラインから入力
    year = input("Year?: ")
    startDay = input("StartDay?: ")
    endDay = input("EndDay?: ")

    if not inputChk(year, startDay, endDay):
        print('---end---')
        sys.exit()

    #tenEva = t2.TenkaiEvaluation(year, startDay, endDay)

    #メイン処理を実行
    execute(year + startDay, year + endDay)


if __name__ == '__main__':
    main()
