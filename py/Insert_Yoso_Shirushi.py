# TFの印1~8と結果コメントを新規テーブル（n_tf_shirushi_and_comment）にInsertする
# TF データ取得対象
#  馬印　　　・・・（root）\TFJV\MY_DATA\UM(yy)(回次)(場名).DAT　　　　　　　　　例）UM225中.DAT
#  馬印2~8　 ・・・（root）\TFJV\MY_DATA\UmaMark2(~8)\UM(yy)(回次)(場名).DAT　　例）UM225中.DAT
#                 　行：当開催の1日目の1R目から　　　例）9日目の11Ｒの場合、 8 * 12 + 11 = 107
#                   列：3から4桁＝レースレベル、7桁目を1番枠として2桁ずつ使用
#  新規テーブル「n_tf_shirushi_and_comment」定義
#              主キー：(yy)(場CD)(回次)(日次)(R番)(馬番)の複合、year、monthday、kettonum、印１～８、コメント、レースレベル、登録日
#  ※ヘッダ無し
#
# 【手順】
# ①入力された期間にある開催情報を取得
# ②古いデータを削除
# ③TFからテキストファイルをロード
# ④テーブルにInsert

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
tf_umashirusi_path = inif.get('csv', 'tf_umashirusi_path')
# print(tf_umashirusi_path)

# メイン処理（年、開始月日、　終了月日）
def execute(year, startday, endday):

    # 処理対象データの削除
    delete_umashirushi(year, startday, endday)

    # 開催情報取得
    kaisai_list = get_kaisai_list(year, startday, endday)

    # 開催リスト分繰り返し
    for i, row in kaisai_list.iterrows():
        # print(year,jyocd,kaiji, nichiji) 
        print(row['Year'], row['MonthDay'], row['JyoCD'], "&" ,  row['Kaiji'], row['Nichiji'],  row['RaceNum'])

        shirushi_lists = []

        # 馬印１～８を取得
        for i in range(8):
            j = i + 1 
            # print("馬印" + str(j))
            # 馬１印、レースレベルを取得
            try:
                shirushi_list = get_shirushi( j , row)
            except Exception as e:
                shirushi_list = ["　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", ]
            shirushi_lists.append(shirushi_list)

        # kettonumリストの取得（対象レース、馬番順）
        kettonum_list = get_kettonum_list(row)
        # shirushi_lists.append(kettonum_list)

        # DataFrame化
        df = pd.DataFrame(shirushi_lists)
        # 馬ごとにするため、DataFrameの行列入れ替える（印リスト）
        df_t  = df.transpose()

        # print(df_t)

        # 印リストと出走馬リストを結合
        df_con = pd.concat([df_t, kettonum_list], axis=1)

        print(df_con)

        # データ登録
        insert_yoso_shirushi(row, df_con)

    print("処理終了")

# kettonumリスト取得対象レース、馬番順
def get_kettonum_list(row):

    # row から対象レース情報取得
    db = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = 'select kettonum, bamei, umaban  from n_uma_race where Year = %(Year)s and monthday = %(MonthDay)s and JyoCD = %(JyoCD)s and racenum = %(RaceNum)s and umaban > "00" order by umaban'
    df = psql.read_sql(sql, conn,
                           params={"Year": row['Year'], "MonthDay": row['MonthDay'], "JyoCD": row['JyoCD'], "RaceNum": row['RaceNum']})

    # print("df　：　出走馬リスト")
    # print(df)
    db.close()

    return df    

# データ登録
def insert_yoso_shirushi(r_info, df_con):

    # receLv
    try:
        raceLv = df_con.iloc[18, 0]
    except:
        raceLv = "　"

    # DFリスト分のデータを登録(出走頭数？)
    for i, df_t in df_con.iterrows():

        # print(df_t)

        try:
            umaban = df_t['umaban']

            if pd.isna(umaban) :
                continue

            yearYY = r_info['Year'][2:]
            kaiji = r_info['Kaiji'][1:]    

            # 
            s1 = df_t.loc[0]
            s2 = df_t.loc[1]
            s3 = df_t.loc[2]
            s4 = df_t.loc[3]
            s5 = df_t.loc[4]
            s6 = df_t.loc[5]
            s7 = df_t.loc[6]
            s8 = df_t.loc[7]

            # データ登録用DataRrame作成
            upsert_data = pd.DataFrame({'Year': [r_info['Year']], 'MonthDay': [r_info['MonthDay']], 'JyoCD': [r_info['JyoCD']],
                                        'RaceNum': [r_info['RaceNum']], 'Umaban': [umaban], 'Kaiji': [kaiji],
                                        'Nichiji': [r_info['Nichiji']], 'YearYY': [yearYY], 'Kettonum': [df_t['kettonum']],
                                        'S1': [s1], 'S2': [s2], 'S3': [s3], 'S4': [s4], 'S5': [s5],
                                        'S6': [s6], 'S7': [s7], 'S8': [s8], 'RaceLV': [raceLv], 'Bamei': [df_t['bamei']]
                                        })
            # print(upsert_data)
            # INSERT処理
            tablename = "n_tf_shirushi_and_comment"
            upsert_data.to_sql(tablename, conn2, if_exists='append', index=False,
                            index_label={'Year', 'MonthDay', 'JyoCD', 'RaceNum', 'Umaban'})

        
        except Exception as e:
            print("insert_yoso_shirushi() 馬印１～８　登録処理で例外発生")
            conn.close()
            print(e)
            return e 

# 馬印１～８（予想印）リストを取得
# 馬印２～８はUmaMark2(~8)のフォルダになる
def get_shirushi(shirusi_idx, row):

    # 馬印１の場合は、フォルダ名はそのまま　２～８の場合はUmaMark2(~8)を付与
    if shirusi_idx is not 1 :
        folder_path = "UmaMark" + str(shirusi_idx)
        tf_umashirusi_path_ = tf_umashirusi_path + "\\" + folder_path + "\\"
    else :
        tf_umashirusi_path_ = tf_umashirusi_path 
    # print (tf_umashirusi_path_)

    # 対象ファイル名を抽出
    # ファイル名  UM(yy)(回次)(場名).DAT UM225中.DAT
    yearYY = row['Year'][2:]
    kaiji = row['Kaiji'][1:]
    jyoMei = convert_jyo_name(row['JyoCD'])

    file_name = "UM" + yearYY + kaiji + jyoMei + ".DAT"

    file_path = tf_umashirusi_path_ + file_name

    # 対象行列の特定
    # 行の特定
    race_row = (int(row['Nichiji']) - 1) * 12 + int(row['RaceNum'])

    # 列の特定
    # shirushi_col = 5 + (row['Umaban'] * 2) 

    shirushi_list = []
    
    # ファイルオープン
    if os.path.exists(file_path):
        with open(file_path) as f:
            lines = f.readlines()
    else:
        # 空文字
        shirushi_list = ["　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", "　", ]
        return shirushi_list
        
    # 該当行抽出
    # 該当行がない場合、処理を終了
    try:
        line = lines[race_row - 1]
    except Exception as e:
        raise

    idx = 6
    byte_line = line.encode('cp932')

    
    # 出走馬の馬印１をリストで取得
    for i in range(18):
        start_idx =  idx + i * 2 
        shirushi = byte_line[start_idx: start_idx + 2]
        # 半角スペース2つを全角スペース１つに変換する
        if shirushi == b'  ' :
            shirushi = "　"
        else :
            shirushi = shirushi.decode('cp932')
        shirushi_list.append(shirushi)

    # レースレベルを追加
    # レースレベル 3から4桁
    if shirusi_idx == 1:
        race_lv_b = byte_line[2: 6 ]
        race_lv = race_lv_b.decode('cp932')
        shirushi_list.append(race_lv)

    return shirushi_list


# 処理対象データの削除
def delete_umashirushi(year, startday, endday):

    db = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = 'delete from n_tf_shirushi_and_comment  where year = %s and MonthDay >= %s and MonthDay <= %s '
    db.execute(sql,(year, startday, endday))
    conn.commit()

# 開催情報取得（年、開始月日、終了月日）
def get_kaisai_list(year, startday, endday):

    db = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = 'select distinct Year, MonthDay, JyoCD, Kaiji, Nichiji, RaceNum from n_race r where year = %(year)s and  ' \
          'MonthDay >= %(startday)s and MonthDay <= %(endday)s and JyoCD <= "10" order by year, MonthDay, JyoCD '
    # db.execute(sql,(row['year'], row['startday'], row['endday']))
    df = psql.read_sql(sql, conn,
                           params={"year": year, "startday": startday, 'endday': endday})

    # print("df　：　対象レース")
    # print(df)
    db.close()

    return df

# 場コードを場名に変換
def convert_jyo_name(jyoCd):
    jyo_name = ""
    if jyoCd == "01" :
        jyo_name = "札"
    elif jyoCd == "02" :
        jyo_name = "函"
    elif jyoCd == "03" :
        jyo_name = "福"
    elif jyoCd == "04" :
        jyo_name = "新"
    elif jyoCd == "05" :
        jyo_name = "東"
    elif jyoCd == "06" :
        jyo_name = "中"
    elif jyoCd == "07" :
        jyo_name = "名"
    elif jyoCd == "08" :
        jyo_name = "京"
    elif jyoCd == "09" :
        jyo_name = "阪"
    elif jyoCd == "10" :
        jyo_name = "小"
    
    return jyo_name

  


# 実行用 コマンドライン引数で 年,月日,現在時間を入力
def main():

    args = sys.argv

    
    year = args[1]
    startday = args[2]
    endday = args[3]
    '''
    year = '2021'
    startday = '0101'
    endday = '0110'
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