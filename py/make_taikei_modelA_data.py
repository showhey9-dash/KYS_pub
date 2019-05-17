#!usr/bin/env python
#
# 体系によるコース特性学習モデルのための学習用データ作成
#
# 【背景】
# モデル開発時にSQLからの取得を設計していたが、取得件数の多さから
# 処理が重くなると考え、取得したデータをcsvに格納することに変更
# （複数のcsvファイルをconcatして学習する方法なら2万件程度ならモデルDで検証済み）

# import
import sys

import MySQLdb
import pandas as pd
import pandas.io.sql as psql

# MySQLの接続情報（各自の環境にあわせて設定のこと）
db_config = {
    'host': 'localhost',
    'db': 'everydb',  # Database Name
    'user': 'root',
    'passwd': 'systemsss',
    'charset': 'utf8',
}


# 学習用体系データ取得＆出力
def getRaceResultWithTaikei(start_day, end_day):
    try:
        print("getRaceResultWithTaikei--start--")

        conn = MySQLdb.connect(host=db_config['host'], db=db_config['db'], user=db_config['user'],
                               passwd=db_config['passwd'], charset=db_config['charset'])

        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        filePath = "../sql/umaTaikei_evaluation2.sql"
        targetSqlFile = open(filePath)
        sql = targetSqlFile.read()  # 終端まで読み込んだdataを返却
        # print(sql)
        print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn, params={"start_day": start_day, "end_day": end_day})
        print("sql実行終了")
        # print(df)

        csv_path = "../csv/taikeiA/" + start_day + "-" + end_day + ".csv"
        df.to_csv(path_or_buf=csv_path, sep=',', header=True, index=False, mode='w', encoding='cp932')

        db.close()
        conn.close()
        return df
    except Exception as e:
        print("学習データの読み込み処理で例外発生")
        print(e)

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


def main():
    # 引数をコマンドラインから入力
    year = input("Year?: ")
    startDay = input("StartDay?: ")
    endDay = input("EndDay?: ")

    if not inputChk(year, startDay, endDay):
        print('---end---')
        sys.exit()

    # メイン処理を実行
    getRaceResultWithTaikei(year + startDay, year + endDay)


if __name__ == '__main__':
    main()