#!usr/bin/env python
#
# 競争結果評価機能
#
# 競争結果に対して人気と着順の開きに着目し、
# 以下のファクター毎に評価を行う。
# ・ハイレベル経験
# ・馬場(良馬場orそれ以外)
# ・頭数(〜9、10〜13、14〜)
# ・平坦コース、急坂コース
# ・U字コース、O字コース
# ・内枠、外枠 ※少頭数に枠の影響はない
# ・長く脚を使う、ギアチェンジ
# ・中山替わり
# ・苦手の好走or得意の凡走
# ・時計がかかる芝
# ・ダート道悪巧者
# ・芝→ダート→芝
# ・ダート→芝→ダート
# ・最後にバテた馬の距離短縮
# ・距離延長
# ・前崩れレースでの残し
# ・前残りレースでの差し損ね
# ・馬体重減からの休養&馬体重増
# ・人気馬と人気薄の結果から馬場のバイアスを考える
#
# 実行単位：同一年における開始日～終了日
#           ※ただし、レース単位でのクエリとなるため、処理時間がかかる想定
#
# インプット：jrdv_uma_race(主テーブル)
#             u_uma_race
# アウトプット：uma_race_eva(テーブル)
#               ファイル名未定(csv)

# import
import sys

import MySQLdb
import pandas.io.sql as psql
from datetime import datetime, timedelta
import linecache
import pandas as pd
import csv

# DB接続
# import mysql.connector as mysqlCon
# from statsmodels.sandbox.tsa.example_arma import ax

db_config = {
    'host': 'localhost',
    'db': 'everydb',  # Database Name
    'user': 'root',
    'passwd': 'systemsss',
    'charset': 'utf8',
}

conn = MySQLdb.connect(host=db_config['host'], db=db_config['db'], user=db_config['user'],
                       passwd=db_config['passwd'], charset=db_config['charset'])

# 評価リスト
syussouba_list = []

# 出走馬評価
race_syussouba_list = []


# 該当日のレース一覧を取得する
# 対象テーブル
def get_race_list(yymmdd):
    race_list = []

    try:
        # カーソル取得
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        sql = "select * from jrdv_race_data where Nengappi = %(yymmdd)s"
        # print(sql)
        # print("sql実行開始")
        # PandasのDataFrameの型に合わせる方法
        df = psql.read_sql(sql, conn, params={"yymmdd": yymmdd})

        # print("sql実行終了")
        # print(df)

        db.close()

        return df
    except Exception as e:
        print("get_race_list() 該当日のレース一覧取得処理で例外発生")
        conn.close()
        print(e)
        return


# レース結果を取得
# 使用SQL[race_result_eva.sql]
def get_race_result(race_info):
    # print("start----get_race_result")

    # 引数から必要な情報を取り出す
    jyo_cd = race_info["jyoCd"]
    year = race_info['Year']
    year_new = '20' + year
    kaiji = race_info['Kaiji']
    nichiji = race_info['Nichiji']
    race_num = race_info['RaceNum']
    # print(year_new)

    # syussouba_list = []

    # カーソル取得
    db = conn.cursor(MySQLdb.cursors.DictCursor)

    file_path = "../sql/race_result_eva.sql"
    target_sql_file = open(file_path)
    sql = target_sql_file.read()  # 終端まで読み込んだdataを返却
    # print(sql)
    # print("sql実行開始")
    # PandasのDataFrameの型に合わせる方法
    df = psql.read_sql(sql, conn, params={"year_new": year_new, "jyocd": jyo_cd, "year": year,
                                          "kaiji": kaiji, "nichiji": nichiji, "racenum": race_num})
    # print("sql実行終了")
    pd.set_option('display.max_columns', 100)
    # print(df)

    db.close()

    return df

# ハイレベル経験
# この実装は劣後対応


# 馬場評価用サブ関数
def baba_eva(x):
    # print(x)
    # 条件：評点 > -3 の場合、馬場状態＋「○」、でない場合「×」
    if x.EvaPoint > -3:
        return str(x.BabaCD) + "○"
    else:
        return str(x.BabaCD) + "×"


# 【未使用】馬場(良馬場orそれ以外)
def baba_eba():

    """
    # 条件：評点 > -3 の場合、馬場状態＋「○」、でない場合「×」
    if race_syussouba_list['EvaPoint'] > -3:
        # race_syussouba_listに列「babaEva」を追加する。値は「馬場状態＋「○」
        baba_eva_str = race_syussouba_list['BabaCd'] + "○"

    if race_syussouba_list['EvaPoint'] < -3:
        # race_syussouba_listに列「babaEva」を追加する。値は「馬場状態＋「×」」
        baba_eva_str = race_syussouba_list['BabaCd'] + "×"
    print(baba_eva_str)
    """
    # baba_eva_str = race_syussouba_list['BabaCD']
    # print(baba_eva_str)

    # DataFrameに列の追加
    # race_syussouba_list['baba_eva'] = race_syussouba_list['EvaPoint'] \
    #    .apply(lambda x: race_syussouba_list['BabaCD'] + "○" if x > -3 else race_syussouba_list['BabaCD'] + "×")
        # .apply(lambda x: "○" if x > -3 else "×")
    # global  race_syussouba_list
    # race_syussouba_list['baba_eva'] = race_syussouba_list.apply(lambda x: baba_eva(x), axis=1)

    print(race_syussouba_list)

    # return


# 頭数(〜9、10〜13、14〜)
def tousu_eva(x):
    # print(x.SyussoTosu)
    # print(type(x.SyussoTosu))
    # 出走頭数の文言を算出
    if x.SyussoTosu <= '09':
        syusso_tosu_str = "少頭"
    elif '10' <= x.SyussoTosu <= '13':
        syusso_tosu_str = "中頭"
    else:
        syusso_tosu_str = "多頭"

    # 条件：評点 > -3 の場合、馬場状態＋「○」、でない場合「×」
    if x.EvaPoint > -3:
        return syusso_tosu_str + "○"
    else:
        return syusso_tosu_str + "×"


# 平坦コース、急坂コース
def course_hei_saka_eva(x):
    # print(x.jyoCd)
    course_hei_saka_str = "平坂ー"

    # 急坂コース：中山(06)、阪神(09)、中京(07)
    # 平坦コース：急坂コース＋東京(05)　！！以外！！
    if x.jyoCd == '06' or x.jyoCd == '07' or x.jyoCd == '09':
        course_hei_saka_str = "急坂"
    elif x.jyoCd == '05':
        # 東京コースはパス
        return course_hei_saka_str
    else:
        course_hei_saka_str = "平坦"

    # 平坦、急坂コースを評価
    # 条件：評点 > -3 の場合、頭数に応じた表現＋「+」、でない場合「-」
    if x.EvaPoint > -3:
        course_hei_saka_str += "○"
    else:
        course_hei_saka_str += "×"

    return course_hei_saka_str


# U字コース、O字コース、L字
def course_uo_eva(x):

    # U字コース：jyuni1～4を見て、jyuni1,2が「00」
    # O字コース：jyuni1が「00」でない
    # L字コース：jyuni1が「00」でjyuni2が「00」でない
    if x.Jyuni1c == '00' and x.Jyuni2c == '00':
        course_uo_str = "U字"
    elif x.Jyuni1c == '00' and x.Jyuni2c != '00':
        course_uo_str = "L字"
    elif x.Jyuni1c != '00':
        course_uo_str = "O字"
    else:
        course_uo_str = "他："

    # 条件：評点 > -3 の場合、頭数に応じた表現＋「○」、でない場合「×」
    if x.EvaPoint > -3:
        course_uo_str += "○"
    else:
        course_uo_str += "×"

    return course_uo_str


# 内枠、外枠 ※少頭数に枠の影響はない9頭以上が対象
def waku_uchi_soto_eva(x):
    # print("waku_uchi_soto_eva()")
    # print(x.wakuban)
    waku_str = "枠ーー"

    # 8頭以下の場合、評価しない
    if x.SyussoTosu < '09':
        return waku_str

        # 内枠かつ4角ポジションが"最"
    if x.wakuban == '1' or x.wakuban == '2':
        if x.Corner4Position == "最":
            waku_str = "内枠"
        else:
            return waku_str
    elif x.wakuban == '7' or x.wakuban == '8':
        if x.Corner4Position == "外" or x.Corner4Position == "大":
            waku_str = "外枠"
        else:
            return waku_str
    else:
        return waku_str

    # 枠内外評価
    # 条件：評点 > -3 の場合、頭数に応じた表現＋「○」、でない場合「×」
    if x.EvaPoint > -3:
        waku_str += "○"
    else:
        waku_str += "×"

    return waku_str


# TODO 脚質評価
def kyakusitsu_eva(x):

    return


# 中山評価
def nakayama_eva(x):
    # print("nakayama_eva--start")

    # jyoCdが中山(06)以外の場合は評価しない
    nakayama_str = "中山"

    if x.jyoCd != '06':
        # print(" This is not Nakayama. ")
        return nakayama_str + "ー"

    # print("This is Nakayama!!!!!!!!")
    # 条件：評点 > -3 の場合、頭数に応じた表現＋「○」、でない場合「×」
    if x.EvaPoint > -3:
        nakayama_str += "○"
    else:
        nakayama_str += "×"

    return nakayama_str


# 距離延短評価
def kyori_en_tan_eva(x):

    kyori_en_tan_str = "距離ーー"

    if x.Zen1Kyori is None:
        return kyori_en_tan_str

    # 前走と今走の距離差を算出
    kyorisa = int(x.Kyori) - int(x.Zen1Kyori)

    # print(x.Kyori)
    # print(x.Zen1Kyori)
    # print(type(int(x.Kyori)))
    # print(type(int(x.Zen1Kyori)))
    # print(kyorisa)

    if kyorisa >= 300:
        kyori_en_tan_str = "距離延"
    elif kyorisa <= -300:
        kyori_en_tan_str = "距離短"
    else:
        return kyori_en_tan_str

    # 条件：評点 > -3 の場合、頭数に応じた表現＋「○」、でない場合「×」
    if x.EvaPoint > -3:
        kyori_en_tan_str += "○"
    else:
        kyori_en_tan_str += "×"

    return kyori_en_tan_str


# 場コードから場名を取得する
def get_jyo_name(jyo_cd):

    if jyo_cd == '01':
        jyo_name = "札"
    elif jyo_cd == '02':
        jyo_name = '函'
    elif jyo_cd == '03':
        jyo_name = '福'
    elif jyo_cd == '04':
        jyo_name = '新'
    elif jyo_cd == '05':
        jyo_name = '東'
    elif jyo_cd == '06':
        jyo_name = '中'
    elif jyo_cd == '07':
        jyo_name = '名'
    elif jyo_cd == '08':
        jyo_name = '京'
    elif jyo_cd == '09':
        jyo_name = '阪'
    elif jyo_cd == '10':
        jyo_name = '小'
    else:
        jyo_name = '他'
    return jyo_name


# 展開評価文言取得
def get_tenkai_mongon(race_syussouba_list):

    # TFのRMark2から該当レースの展開評価を抽出する
    tf_rmark2_path = "C:\\TFJV\\MY_DATA\\RMark2\\"

    # ファイル名とフィルパスの作成
    # 場名取得
    jyo_name = get_jyo_name(race_syussouba_list['jyoCd'])
    year = race_syussouba_list['Year']
    kaiji = race_syussouba_list['Kaiji']

    file_name = "RM" + year + kaiji + jyo_name + ".DAT"
    path = tf_rmark2_path + file_name

    # 行番号 = nur_nichiji(n_uma_race)
    nur_nichiji = int(race_syussouba_list['nur_nichiji'])

    # linecasheモジュールを使用して指定行の文字列を取得する
    # 参考 https://qiita.com/Kodaira_/items/eb5cdef4c4e299794be3
    target_line = linecache.getline(path, nur_nichiji)
    # print(type(target_line))
    # レース番号を基に評価文言を取得
    race_num = int(race_syussouba_list['RaceNum'])
    # 取得列の計算　1R:3桁目、2R:9桁目、、、12R:69桁目
    # 文字分割の第1引数は0始まり、第2引数は1から数える（8文字目なら「8」）
    column = 6 * (race_num -1) + 2
    # print("column = " + str(column))
    race_tenkai_mongon = target_line[column: column + 4]
    # print("race_tenkai_mongon = " + race_tenkai_mongon)

    return race_tenkai_mongon


# 前残し評価
def maenokoshi_eva(x):

    kyakusitsu_str = "ハイペー"

    # 完走チェック
    if x.TimeDiff is None or x.TimeDiff == "   " :
        return kyakusitsu_str

    # レース展開文言取得
    race_tenkai_mongon = get_tenkai_mongon(x)

    # 展開評価が「KOHO」かつ、レース脚質が「逃」「先」の場合
    if race_tenkai_mongon == "KOHO" and (x.RaceKyakusitsu == '逃' or x.RaceKyakusitsu == '先'):
        kyakusitsu_str = "ハイペ"
    else:
        return kyakusitsu_str

    if int(x.TimeDiff) >= -1.0:
        # 着差が「-1.0以内」の場合、「○」
        kyakusitsu_str += "○"
    else:
        # 着差が「-1.0以上」の場合、「×」
        kyakusitsu_str += "×"

    return kyakusitsu_str


# 差し損ね評価
def sashi_sokone_eva(x):

    kyakusitsu_str = "差損ねー"

    # 完走チェック
    if x.TimeDiff is None or x.TimeDiff == "   ":
        return kyakusitsu_str

    # レース展開文言取得
    race_tenkai_mongon = get_tenkai_mongon(x)
    # print("race_tenkai_mongon = " + race_tenkai_mongon)

    if race_tenkai_mongon == "SENK" and (x.RaceKyakusitsu == '差' or x.RaceKyakusitsu == '追'):
        kyakusitsu_str = "差損ね"
    else:
        return kyakusitsu_str

    if int(x.HaronTimeL3Jyuni) <= 3:
        # 上り3Fがメンバー中3位以内なら
        kyakusitsu_str += '○'
    else:
        kyakusitsu_str += '×'
    
    # print(x.HaronTimeL3Jyuni)
    # print(kyakusitsu_str)

    return kyakusitsu_str


# 出走馬を評価する
def eva_syussouba(race_info):
    print("eva_syussouba()")

    # グローバル変数の宣言と初期化
    global race_syussouba_list
    race_syussouba_list = []

    # 該当レースの出走馬リストを取得(関数利用)
    race_syussouba_list = get_race_result(race_info)

    # TODO ハイレベル評価
    # print('ハイレベル評価（未実装）')
    race_syussouba_list['level_eva'] = "高Ｌｖー"

    # 馬場評価(関数利用)
    # print("馬場評価")
    race_syussouba_list['baba_eva'] = race_syussouba_list.apply(lambda x: baba_eva(x), axis=1)

    # 頭数評価(関数利用)
    # print("頭数評価")
    race_syussouba_list['tousu_eva'] = race_syussouba_list.apply(lambda x: tousu_eva(x), axis=1)

    # コース平坦急坂評価(関数利用)
    # print("頭数評価")
    race_syussouba_list['course_hei_saka_eva'] = race_syussouba_list.apply(lambda x: course_hei_saka_eva(x), axis=1)

    # コースU字O字評価(関数利用)
    # print("コースU字O字評価")
    race_syussouba_list['course_uo_eva'] = race_syussouba_list.apply(lambda x: course_uo_eva(x), axis=1)

    # 内枠外枠評価(関数利用)
    # print("内枠外枠評価")
    race_syussouba_list['waku_utisoto_eva'] = race_syussouba_list.apply(lambda x: waku_uchi_soto_eva(x), axis=1)

    # TODO 脚質評価(関数利用)
    # print('脚質評価（未実装）')
    race_syussouba_list['kyaku_eva'] = "脚質ー"

    # 中山評価(関数利用)
    # print("中山評価")
    race_syussouba_list['nakayama_eva'] = race_syussouba_list.apply(lambda x: nakayama_eva(x), axis=1)

    # 距離延短評価(関数利用)
    # print("距離延短評価")
    race_syussouba_list['kyori_en_tan_eva'] = race_syussouba_list.apply(lambda x: kyori_en_tan_eva(x), axis=1)

    # 前残し評価(関数利用)
    # print("前残し評価")
    race_syussouba_list['maenokoshi_eva'] = race_syussouba_list.apply(lambda x: maenokoshi_eva(x), axis=1)

    # 差し損ね評価(関数利用)
    # print("差し損ね評価")
    race_syussouba_list['sashi_sokone_eva'] = race_syussouba_list.apply(lambda x: sashi_sokone_eva(x), axis=1)

    # print(race_syussouba_list)


# csv出力：評価情報を出力
def to_csv_eva_result(model_type, nengappi):
    print("to_csv_eva_result")

    global race_syussouba_list

    # df = pd.DataFrame(race_syussouba_list)

    # 集計一覧ファイルを読み込む（csv）
    # outputディレクトリ以下のパスを指定
    race_num = race_syussouba_list['RaceNum'].iloc[0]
    jyo_cd = race_syussouba_list['jyoCd'].iloc[0]
    data_path = "../output/csv/" + model_type + "/" + model_type + "_" + nengappi + jyo_cd + race_num + ".csv"

    # ファイル読み込み
    # file1 = pd.read_csv(data_path, engine='python', encoding='cp932', index_col=None, dtype='object')
    # print(file1)
    # 集計一覧ファイルに書き込む
    # file1 = pd.concat([file1, race_syussouba_list])
    # print(data_path)
    # print(race_syussouba_list)
    race_syussouba_list.to_csv(data_path)
    # file1.to_csv(data_path, columns=['nengappi','win_rate','continuous_rate','double_win_rate',
    #                                 'win_recovery_rate','double_win_recovery_rate'])


# race_syussouba_listをuma_race_evaテーブルに登録する列だけにスリム化する
def slim_down_race_syussouba_list():

    global race_syussouba_list

    # print(race_syussouba_list)

    race_syussouba_list_light = race_syussouba_list.drop(['nur_nichiji', 'wakuban', 'SyussoTosu', 'RaceName', 'Grade', 'Jyoken', 'Deokure', 'Furi', 'BabaCD', 'Kyori', 'Jyuni1c', 'Jyuni2c', 'Jyuni3c', 'Jyuni4c', 'RaceKyakusitsu', 'DouchuUchiSoto', 'Corner4Position', 'GoalUchiSoto', 'zen1seisekiKey', 'zen1raceKey', 'WinBamei', 'TimeDiff', 'HaronTimeL3', 'HaronTimeL3Jyuni', 'HaronTimeL3Sa', 'HaronTimeL3UchiSoto', 'Zen1JyoCd', 'Zen1Year', 'Zen1Kaiji', 'Zen1Nichiji', 'Zen1RaceNum', 'Zen1RaceName', 'Zen1Grade', 'Zen1Jyoken', 'Zen1KakuteiJyuni', 'Zen1Ninki', 'Zen1Kyori', 'Zen1TrackCD', 'Zen1BabaCD'], axis=1)
    # print(race_syussouba_list_light)
    return race_syussouba_list_light


# テーブル登録[uma_race_eva]
def insert_uma_race_eva(race_syussouba_list_light):

    print('start--insert_uma_race_eva')

    # startTime = datetime.now()
    # カーソル取得
    # cur = conn.cursor(buffered=True)
    cur = conn.cursor()
    # %sの数は動確のためreadKyhFileで抽出する項目数と一致させる
    # sql ='INSERT INTO jrdv_uma_taikei (`jyoCd`,`Year`,`Kaiji`,`Nichiji`,`RaceNum`,`Umaban`,`KettoNum`,`Bamei`,`SouhouCode`,`TaikeiCode`,`TaikeiSougou1Code`,`TaikeiSougou2Code`,`TaikeiSougou3Code`,`UmaTokki1Code`,`UmaTokki2Code`,`UmaTokki3Code`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    sql = 'INSERT INTO uma_race_eva VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,curdate())'

    param = []
    # print("start:登録データ作成" + str(datetime.now()))
    for data in race_syussouba_list_light.iterrows():
        # print(type(data))
        # print(data[1])
        param.append([data[1][0], data[1][1], data[1][2], data[1][3], data[1][4], data[1][5], data[1][6], data[1][7], data[1][8], data[1][9], data[1][10],
                      data[1][11], data[1][12], data[1][13], data[1][14], data[1][15], data[1][16], data[1][17], data[1][18], data[1][19], data[1][20], data[1][21], data[1][22]])
    # print("end:登録データ作成" + str(datetime.now()))
    # リスト分実行
    # SQL作成、発行（insertmany関数使えるか？）
    try:
        cur.executemany(sql, param)
        conn.commit()
    except Exception as e:

        print(e)


# 次走コメント欄記載文字列作成
# レース単位
def make_kj_commnet(df):

    df2 = pd.DataFrame()

    # 形式：主キー,コメント
    df_key = df['Nengappi'] + df['jyoCd'] + "0" + df['Kaiji'] + df['nur_nichiji'] + df['RaceNum'] + df['Umaban']
    df_com = df['level_eva'] + df['baba_eva'] + df['tousu_eva'] + df['course_hei_saka_eva'] + df['course_uo_eva'] + df['waku_utisoto_eva'] + df['kyaku_eva'] + df['nakayama_eva'] + df['kyori_en_tan_eva'] + df['maenokoshi_eva'] + df['sashi_sokone_eva']

    # print(df_key)
    # print(df_com)

    df2['df_key'] = df_key
    df2['df_com'] = df_com

    return df2


# TF書き込み[K次メモ]
def write_tf_kol_comment(nengappi):
    global race_syussouba_list

    # 旧レースID（201905260502121107：年月日、場code99、回次99、日次99、レース番号99、馬番99）

    year = nengappi[:4]
    # 対象ファイルを特定
    fileDir = 'C:\TFJV\MY_DATA\KOL2_COM\\' + year

    # 参考
    # race_num = race_syussouba_list['RaceNum'].iloc[0]
    # jyo_cd = race_syussouba_list['jyoCd'].iloc[0]

    # raceIdをstrに
    jyo_cd = race_syussouba_list['jyoCd'].iloc[0]
    year_yy = race_syussouba_list['Year'].iloc[0]
    kaiji = race_syussouba_list['Kaiji'].iloc[0]
    nichiji = race_syussouba_list['Nichiji'].iloc[0]
    file_id = jyo_cd + year_yy + kaiji + nichiji
    # print(file_id)

    # K次走コメントファイル名
    k2_file_name = "K2" + file_id + ".dat"
    # ファイルパス
    file_path = fileDir + '\\' + k2_file_name
    # print(file_path)

    # 次走コメント欄記載文字列作成
    kj_comment_list = make_kj_commnet(race_syussouba_list)
    # print(kj_comment_list)
    kj_comment_list.to_csv(file_path, index=False, header=False, encoding='cp932',index_label=None, mode='a')


"""
    newLine = ''
    lineIndex = 0

    # ファイル読み込み、ファイル内からレースＩＤを検索
    # with句はファイル閉じるを兼ねる
    with open(file_path, 'r') as f1:

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
    with open(file_path, 'a') as f3:
        f3.writelines()
"""


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
    # 対象モデル名
    # model_type = 'taikeiA'

    startTime = datetime.now()
    print("start:eva_race_result" + str(startTime))

    # 引数をdate型に
    fromDate = datetime(int(fromDateStr[0:4]), int(fromDateStr[4:6]), int(fromDateStr[6:8]))
    toDate = datetime(int(toDateStr[0:4]), int(toDateStr[4:6]), int(toDateStr[6:8]))

    # カレンダーとしてのカウンター
    delta = timedelta(days=1)

    # 番組情報取得、レース結果を評価し、uma_race_evaへ登録
    while fromDate <= toDate:
        # print(fromDate.strftime("%Y%m%d"))
        bacDate = fromDate.strftime("%Y%m%d")[:8]

        # 該当日のレース一覧を取得
        race_list = get_race_list(bacDate)
        if race_list is None:
            fromDate += delta
            continue

        print("---レース日------------------" + bacDate)

        # レースごとに出走馬を評価する

        for index, race_info in race_list.iterrows():
            try:
                print(index)
                # print(type(race_info))
                # 出走馬を評価する
                eva_syussouba(race_info)
                # 評価結果をcsvに出力する
                to_csv_eva_result("eva_race_result", bacDate)

                # race_syussouba_listをテーブルに登録する分だけにスリム化する
                race_syussouba_list_light = slim_down_race_syussouba_list()

                # 評価結果をテーブルに登録／更新する
                insert_uma_race_eva(race_syussouba_list_light)

                # 評価結果をTFに書き込む
                write_tf_kol_comment(bacDate)

            except ZeroDivisionError:
                print("該当レースなし")
                continue
            except Exception as e:
                print("レース毎の出走馬評価中に例外発生")
                print(e)
                continue

        fromDate += delta

    conn.close()
    endTime = datetime.now()
    print("end:eva_race_result" + str(startTime))
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