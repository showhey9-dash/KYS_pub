# 体系によるコース特性学習モデル
#
# JRDVの体系コードとレース結果を用いてコース毎の特性をつかむ
# 当日出走する馬達の中から体系的にマッチするものを抽出する
# 2019.04.26
#
# 使用data：
#   JRDVレース結果（テーブル名JRDV_UMA_RACE）
#   JRDV体系　　　（テーブル名JRDV_UMA_TAIKEI）
#     体系データの作成はJupyterNotebookの競走馬_馬体走法変換データ作成.ipynbにて実施
#
# 学習単位（競馬場、芝orダート、内外、左右、距離）
# 正解データ：理想は標準データから+0.5以内
# 　　　　　だけど、標準データがないので4着以内を評価する（1）
# 　　　　　注釈①：抽出データから出遅れ、不利のある馬は除外
# 　　　　　注釈②：出遅れ、不利を受けても4着以内なら評価する（1）　　
# 　　　　　注釈③：対象クラスは1000万以上クラス（下級クラスはノイズが多そう）
# 前処理 該当日に出走する馬たちのデータをJRDV_UMA_TAIKEIに登録しておく
#       ソース：競走馬_馬体走法変換データ作成.ipynb

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
    'passwd': '*****',
    'charset': 'utf8',
}


# 学習データ 読み込み
# csvファイルを取得
def getRaceCsvWithTaikei():
    print('getRaceCsvWithTaikei--start')
    path = "../csv/taikeiA"
    files = os.listdir(path)
    files_file = [f for f in files if os.path.isfile(os.path.join(path, f))]
    # print(files_file)

    fileAll = pd.DataFrame()
    # files = []
    # print(fileAll)
    for fileName in files_file:
        # 学習データファイルの読み込み（リスト分）
        # print(type(fileName))
        filePath = '../csv/taikeiA/' + fileName
        file1 = pd.read_csv(filePath, engine='python', encoding='cp932', index_col=None, dtype='object')
        # 欠損行を削除
        file1 = file1.dropna(how='any')
        # print(type(file1))
        # print(len(file1))
        # print(file1)
        # ファイルの結合
        fileAll = pd.concat([fileAll, file1])
        # files.append(file1)
    # fileAll = pd.concat(files)

    # fileAll = fileAll['TaikeiTekisei'].astype(int)

    print(fileAll.dtypes)
    print(len(fileAll))
    fileAll.to_csv('sample20190501.csv')
    return fileAll


# 学習用体系データ取得（実際は使用しない）
def getRaceResultWithTaikei(nengappi):
    try:
        print("getRaceResultWithTaikei--start--")

        conn = MySQLdb.connect(host=db_config['host'], db=db_config['db'], user=db_config['user'],
                               passwd=db_config['passwd'], charset=db_config['charset'])

        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        filePath = "../sql/umaTaikei_evaluation.sql"
        targetSqlFile = open(filePath)
        sql = targetSqlFile.read()  # 終端まで読み込んだdataを返却
        # print(sql)
        print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn, params={"nengappi": nengappi}, )
        print("sql実行終了")
        # 取得したTaikeiTekiseiの型をobjectに変換（csvと結合すると学習時に型ミスマッチが起こる
        # df = df.astype(object)
        # print(df)

        db.close()
        conn.close()
        # print(df.dtypes)
        # print(len(df))
        return df
    except Exception as e:
        print("学習データの読み込み処理で例外発生")
        print(e)


# 体系コードを2つの塊に分割する
# TaikeiCodeを[0]～[5]と[10]～[16]に分割して列を追加
def split2_taikei_code(df):
    # print(df.dtypes)
    taikei_0to5 = df['TaikeiCode'].str[:5]
    # print(df['TaikeiCode'].str[:5])
    taikei_10to16 = df['TaikeiCode'].str[10:16]
    # print(taikei_10to16)

    # 列の追加
    df['taikei_0to5'] = taikei_0to5
    df['taikei_10to16'] = taikei_10to16

    # print(df.dtypes)
    # print(df)
    return df


# 体系コードを2つの塊に分割する
# TaikeiCodeの[0]～[5]と[10]～[16]をそれぞれに分割して列を追加
def split_all_taikei_code(df):
    # print(df.dtypes)
    taikei_0 = df['TaikeiCode'].str[0]
    taikei_1 = df['TaikeiCode'].str[1]
    taikei_2 = df['TaikeiCode'].str[2]
    taikei_3 = df['TaikeiCode'].str[3]
    taikei_4 = df['TaikeiCode'].str[4]
    taikei_5 = df['TaikeiCode'].str[5]
    taikei_10 = df['TaikeiCode'].str[10]
    taikei_11 = df['TaikeiCode'].str[11]
    taikei_12 = df['TaikeiCode'].str[12]
    taikei_13 = df['TaikeiCode'].str[13]
    taikei_14 = df['TaikeiCode'].str[14]
    taikei_15 = df['TaikeiCode'].str[15]

    # 列の追加
    df['taikei_0'] = taikei_0
    df['taikei_1'] = taikei_1
    df['taikei_2'] = taikei_2
    df['taikei_3'] = taikei_3
    df['taikei_4'] = taikei_4
    df['taikei_5'] = taikei_5
    df['taikei_10'] = taikei_10
    df['taikei_11'] = taikei_11
    df['taikei_12'] = taikei_12
    df['taikei_13'] = taikei_13
    df['taikei_14'] = taikei_14
    df['taikei_15'] = taikei_15

    # print(df.dtypes)
    # print(df)
    return df


# モデルの学習
def learnTaileiTekisei(nengappi):
    print('learnTaileiTekisei--start')
    # 2017、2019年のデータをcsvから取得①
    # df1 = getRaceCsvWithTaikei()
    # 2019年の試験データ前日までのデータを取得②
    df = getRaceResultWithTaikei(nengappi)
    # ①と②を結合
    # df = pd.concat([df1, df2])
    # print(df.dtypes)
    # print(len(df))
    # 体系コードを分割
    df = split_all_taikei_code(df)
    # df.to_csv('learnTaileiTekisei_before_split.csv')
    # 学習データをさらに学習用と試験用に分割する
    X_setsumei = df.drop('TaikeiTekisei', 1)
    X_setsumei = X_setsumei.drop('BabaCD', 1)
    X_setsumei = X_setsumei.drop('TaikeiCode', 1)
    X_setsumei.to_csv('sample20190501_2.csv')
    Y_mokuteki = df.TaikeiTekisei
    # print(Y_mokuteki.dtypes)
    # print(X_setsumei)
    # print(Y_mokuteki)
    X_setsumei_train, X_setsumei_test, Y_mokuteki_train, Y_mokuteki_test = ms.train_test_split(X_setsumei, Y_mokuteki,
                                                                                               test_size=0.3)
    model = DecisionTreeClassifier(max_depth=8, random_state=0)
    # 学習
    model.fit(X_setsumei, Y_mokuteki)

    # 学習データからの予測値
    predict_train = model.predict(X_setsumei_train)
    # csvファイルとして保存
    predict_train_df = pd.DataFrame(predict_train)
    predict_train_df.to_csv('../output/out_predict_train.csv')
    # テストデータからの予測値
    predict_test = model.predict(X_setsumei_test)
    # print(predict_test)
    # スコア
    score = model.score(X_setsumei_test, Y_mokuteki_test)
    print(score)
    return model


# 当日出走馬情報の取得
def getSyusoubaList(nengappi):
    try:
        print("getSyusoubaList--start--")

        conn = MySQLdb.connect(host=db_config['host'], db=db_config['db'], user=db_config['user'],
                               passwd=db_config['passwd'], charset=db_config['charset'])

        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)
        filePath = "../sql/umaTaikei_evaluation_syussou_list.sql"
        targetSqlFile = open(filePath)
        sql = targetSqlFile.read()  # 終端まで読み込んだdataを返却
        # print(sql)
        print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        test_df = psql.read_sql(sql, conn, params={"nengappi": nengappi})
        print("sql実行終了")
        # print(range(test_df['Bamei'].size))
        # 体系コードを分割
        test_df = split_all_taikei_code(test_df)

        # resultをdataframe作成
        result2 = pd.DataFrame({'No.': range(test_df['Bamei'].size)})
        # test_df['No.'] = range(test_df.size)
        # print(result2)

        test_df2 = pd.concat([result2, test_df], axis=1)

        db.close()
        conn.close()
        # print(test_df2)
        return test_df2
    except Exception as e:
        print("学習データの読み込み処理で例外発生")
        print(e)


def test_df_trim(test_df):
    # TaikeiCode分割対応によりTaikeiCodeをdrpo対象に追加
    test_df_trim = test_df.drop(['No.', 'TaikeiCode', 'Year', 'Kaiji', 'Nichiji', 'RaceNum', 'Umaban', 'Bamei'], 1)
    # print(test_df_trim)
    return test_df_trim


# テストデータの評価
def model_evaluation(model, test_df, test_df_trim):
    # テストデータを評価
    result = model.predict(test_df_trim)
    # print(result)
    # result.to_csv('tmp_result/taikeitekiseimodel.csv')

    # csvファイルとマージ
    # resultをdataframe作成
    result2 = pd.DataFrame({'key': result, 'No.': range(result.size)})
    # print(result2)

    data3 = pd.merge(result2, test_df, left_on='No.', right_on='No.')
    # print(data3)
    return data3


# Targetの予想コメント末尾に「　モデルＸ推奨馬」と記載
def writeYosouComment(modelType, raceId):

    # 旧レースID（0618580101：場code、年YY、回次Z、日次Z、レース番号99、馬番99）

    year = "2019"
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


# 該当馬を出力（TFへ書き込みetc）
# 該当馬のみ抽出
def output(data3, nengappi):
    print('output--start')
    raceIdList = pd.DataFrame()
    for umaData in data3.iterrows():
        # print(type(umaData))
        umaDataCol = umaData[1]
        # print(umaDataCol['key'])
        if umaDataCol['key'] == 1:
            print(umaDataCol['jyoCd'], umaDataCol['RaceNum'], umaDataCol['Umaban'], umaDataCol['Bamei'])
            # Targetの予想コメントの末尾に「　モデルＸ推奨馬」と記載したい
            raceId = umaDataCol['jyoCd'] + umaDataCol['Year'] + umaDataCol['Kaiji'] + umaDataCol['Nichiji'] + umaDataCol['RaceNum'] + umaDataCol['Umaban']
            # print(raceId)
            #print(type(raceId))
            writeYosouComment(" 体系モデルA推奨馬", raceId)
            raceId_index = pd.DataFrame([[umaDataCol['jyoCd'], umaDataCol['Year'], umaDataCol['Kaiji'], umaDataCol['Nichiji'], umaDataCol['RaceNum'], umaDataCol['Umaban'], umaDataCol['Bamei']]], columns=['jyoCd','Year','Kaiji','Nichiji','RaceNum','Umaban','Bamei'] )
            #print(raceId_index)
            raceIdList = pd.concat([raceIdList, raceId_index])
    # csv出力
    path = "../output/csv/taikeiA/" + nengappi + '.csv'
    raceIdList.to_csv(path)


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


# メインメソッドの流れ
# import
def execute(fromDateStr, toDateStr):
    startTime = datetime.now()
    print("start:model_taikei_tekisei_A" + str(startTime))

    # 引数をdate型に
    fromDate = datetime(int(fromDateStr[0:4]), int(fromDateStr[4:6]), int(fromDateStr[6:8]))
    toDate = datetime(int(toDateStr[0:4]), int(toDateStr[4:6]), int(toDateStr[6:8]))

    # カレンダーとしてのカウンター
    delta = timedelta(days=1)

    # 番組情報取得、DB登録処理
    while fromDate <= toDate:

        bacDate = fromDate.strftime("%Y%m%d")[:8]
        print(bacDate)

        # 当日出走馬情報の取得
        test_df = getSyusoubaList(bacDate)
        if test_df.empty:
            print('当日出走情報なし')
            fromDate += delta
            continue
        else:
            # モデルのスコアを算出
            model = learnTaileiTekisei(bacDate)

        test_df_trim_af = test_df_trim(test_df)
        # テストデータの評価
        data3 = model_evaluation(model, test_df, test_df_trim_af)
        # 該当馬を出力（TFへ書き込みetc）
        output(data3, bacDate)

        fromDate += delta

    # f = tree.export_graphviz(model, out_file='../dot/kys_model_taikeiA.dot', filled=True, rounded=True)
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

    """
    # 引数をコマンドラインから入力
    # year = input("Year?: ")
    startDay = input("Year is '2019'. What is the Date?: ")
    # endDay = input("EndDay?: ")

    if not inputChk('2019', startDay, startDay):
        print('---end---')
        sys.exit()

    # メイン処理を実行
    execute('2019' + startDay)
    """


if __name__ == '__main__':
    main()

