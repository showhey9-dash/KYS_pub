#!usr/bin/env python
#
# 体系適正モデル集計処理
#
# 算出したモデル該当馬の競争結果を抽出し、
# 各競走馬毎の結果を日別に出力する。
# また、勝率や回収率なども可視化する
#
# インプット：各モデルの出力されたcsv

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

# 各モデルの注対象馬ごとの着順、払い戻しを取得
# 払戻データをcsvに出力する
def get_model_extra_list(model_type, nengappi):

    # outputディレクトリ以下のパスを指定
    data_path = "../output/csv/" + model_type + "/" + nengappi + '.csv'

    # カーソル取得
    db = conn.cursor(MySQLdb.cursors.DictCursor)

    filePath = "../sql/race_harai.sql"
    targetSqlFile = open(filePath)
    sql = targetSqlFile.read()  # 終端まで読み込んだdataを返却
    # print(sql)

    # ファイル読み込み
    try:
        file1 = pd.read_csv(data_path, engine='python', encoding='cp932', index_col=None, dtype='object')
    except Exception as e:
        print('払戻情報なし')
        print(e)
        return None

    # ファイル内の件数分、競争結果の取得

    race_result_list = pd.DataFrame()

    print("sql実行開始")
    for index, data in file1.iterrows():
        #print(type(data))
        #print(data)
        # 競争結果と払戻情報の取得
        jyocd = data['jyoCd']
        year = '20' + data['Year']
        kaiji = '0' + data['Kaiji']
        nichiji = '0' + data['Nichiji']
        racenum = data['RaceNum']
        umaban = data['Umaban']

        #print(jyocd, year, kaiji, nichiji, racenum, umaban)

        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn,
                           params={"jyocd": jyocd, "year": year, "kaiji": kaiji, "nichiji": nichiji,
                                   "racenum": racenum, "umaban": umaban}, )
        # print(df)
        race_result_list = pd.concat([race_result_list, df])
    print("sql実行終了")

    # csv出力（する必要ある？）
    path = "../extraction/csv/" + model_type + "/" + nengappi + '.csv'
    race_result_list.to_csv(path)

    return race_result_list


# 勝率、複勝率と各回収率を計算
# 年月日
def aggre_kyosou_seiseki(race_result_list, nengappi):
    race_syussou_count = 0
    win_count = 0
    # 連対：Consecutive pair
    pair_count = 0
    double_win_count = 0
    tan_sum = 0
    fuku_sum = 0

    # race_result_list分繰り返す
    for idx, rr in race_result_list.iterrows():

        # 該当馬の着順を基に単勝と複勝の値を抽出する
        tyakujyun = rr['KakuteiJyuni']

        if tyakujyun == '01':
            win_count += 1
            pair_count += 1
            double_win_count += 1
        elif tyakujyun == '02':
            pair_count += 1
            double_win_count += 1
        elif tyakujyun == '03':
            double_win_count += 1

        umaban = rr['Umaban']
        # print(tyakujyun)

        tan_1 = rr['PayTansyoUmaban1']
        tan_2 = rr['PayTansyoUmaban2']
        fuku_1 = rr['PayFukusyoUmaban1']
        fuku_2 = rr['PayFukusyoUmaban2']
        fuku_3 = rr['PayFukusyoUmaban3']
        fuku_4 = rr['PayFukusyoUmaban4']
        fuku_5 = rr['PayFukusyoUmaban5']

        # 抽出値を累計する
        # 単勝チェック
        if umaban == tan_1:
            tan_sum += int(rr['PayTansyoPay1'])
        elif umaban == tan_2:
            # tan_pay_2= rr['PayTansyoPay2'].astype(int)
            tan_sum += int(rr['PayTansyoPay2'])
        else:
            pass

        # 複勝チェック
        if umaban == fuku_1:
            # fuku_pay_1 += rr['PayFukusyoPay1'].astype(int)
            fuku_sum += int(rr['PayFukusyoPay1'])
        elif umaban == fuku_2:
            # fuku_pay_2 = rr['PayFukusyoPay2'].astype(int)
            fuku_sum += int(rr['PayFukusyoPay2'])
        elif umaban == fuku_3:
            # fuku_pay_3 = int(rr['PayFukusyoPay3'])
            # print(type(fuku_pay_3))
            fuku_sum += int(rr['PayFukusyoPay3'])
        elif umaban == fuku_4:
            # fuku_pay_4= rr['PayFukusyoPay4'].astype(int)
            fuku_sum += int(rr['PayFukusyoPay4'])
        elif umaban == fuku_5:
            # fuku_pay_5= rr['PayFukusyoPay5'].astype(int)
            fuku_sum += int(rr['PayFukusyoPay5'])
        else:
            pass

        race_syussou_count += 1

    # print(race_syussou_count)
    # print(win_count)
    # print(pair_count)
    # print(double_win_count)
    # print(tan_sum)
    # print(fuku_sum)
    # リスト内の勝率、連帯率、複勝率、単勝回収率、複勝回収率を計算する
    print(win_count, (pair_count - win_count), (double_win_count - pair_count), (race_syussou_count - double_win_count))

    # 日付をindexにして率リスト.csvの末行に追記する
    # 勝率
    win_rate = np.round(win_count / race_syussou_count * 100, decimals=2)
    # 連対率
    continuous_rate = np.round(pair_count / race_syussou_count * 100, decimals=2)
    # 複勝率
    double_win_rate = np.round(double_win_count / race_syussou_count * 100, decimals=2)
    # 単勝回収率
    win_recovery_rate = np.round(tan_sum / race_syussou_count, decimals=2)
    # 複勝回収率
    double_win_recovery_rate = np.round(fuku_sum / race_syussou_count, decimals=2)
    """
    else:
        win_rate = 0
        continuous_rate = 0
        double_win_rate = 0
        win_recovery_rate = 0
        double_win_recovery_rate = 0
    """
    # print(win_rate)
    # print(continuous_rate)
    # print(double_win_rate)
    print(win_recovery_rate)
    print(double_win_recovery_rate)

    # DataFrame型に設定して返却
    # columns=['nengappi','win_rate','continuous_rate','double_win_rate','win_recovery_rate','double_win_recovery_rate']
    result = pd.DataFrame([[nengappi, win_rate, continuous_rate, double_win_rate,
                            win_recovery_rate, double_win_recovery_rate]],
                          columns=['nengappi', 'win_rate', 'continuous_rate', 'double_win_rate',
                                   'win_recovery_rate', 'double_win_recovery_rate'])
    # print(result.dtypes)
    # print(result)

    return result


# csv出力：集計一覧ファイルに追記
def write_aggre_list(aggre_result, model_type, nengappi):
    # 集計一覧ファイルを読み込む（csv）
    # outputディレクトリ以下のパスを指定
    data_path = "../extraction/csv/" + model_type + "/" + model_type + "_aggre_results.csv"

    # ファイル読み込み
    file1 = pd.read_csv(data_path, engine='python', encoding='cp932', index_col=None, dtype='object')
    # print(file1)
    # 集計一覧ファイルに書き込む
    file1 = pd.concat([file1, aggre_result])
    print(file1)
    file1.to_csv(data_path, columns=['nengappi','win_rate','continuous_rate','double_win_rate',
                                     'win_recovery_rate','double_win_recovery_rate'])


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
def execute(fromDateStr, toDateStr, model_type):

    # 対象モデル名
    # model_type = 'taikeiA'

    startTime = datetime.now()
    print("start:aggre_model" + model_type + " " + str(startTime))

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

        # 各モデルの注対象馬ごとの着順、払い戻しを取得
        # 払戻データをcsvに出力する
        race_result_list = get_model_extra_list(model_type, bacDate)
        if race_result_list is None:
            fromDate += delta
            continue

        # 勝率、複勝率と各回収率を計算
        try:
            aggre_result = aggre_kyosou_seiseki(race_result_list, bacDate)
            # 計算結果をcsvに出力する
            write_aggre_list(aggre_result, model_type, bacDate)
        except ZeroDivisionError:
            print("該当レースなし")
            continue

        fromDate += delta

    endTime = datetime.now()
    print("end:aggre_model" + model_type + " " + str(startTime))
    print("処理時間" + str(endTime - startTime))


def main():
    #引数をコマンドラインから入力
    year = input("Year?: ")
    startDay = input("StartDay?: ")
    endDay = input("EndDay?: ")
    model_type = input("ModelType?: ")

    if not inputChk(year, startDay, endDay):
        print('---end---')
        sys.exit()

    #メイン処理を実行
    execute(year + startDay, year + endDay, model_type)
    
if __name__ == '__main__':
    main()