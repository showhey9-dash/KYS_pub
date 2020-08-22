
# coding: utf-8

# In[1]:

# 2018/12/20 着手
# 競争結果から先行有利 or 後方有利 の評価を行い
# TargetFrontierのR印2に評価を付けていく。また、同様の情報をcsvにも出力する
# 次フェーズはこれらの情報で展開予測モデルを構築したい
#
# 処理の流れ
#  指定日のレース結果から上位5着馬の4角順位を取得する---A
#  Aを基に 先行有利 or 後方有利 or 標準 を評価する---B
#  BをTargetFrontierに書き込む---C
#  Cをcsvに出力する（ファイルの単位やファイル名は未検討）


# In[2]:

#  指定日のレース結果から上位5着馬の4角順位を取得する---A
# DB接続
import MySQLdb
import os
import pandas as pd
import shutil
import glob
import pathlib
import pprint
from datetime import datetime, timedelta

# MySQLの接続情報（各自の環境にあわせて設定のこと）
db_config = {
    'host': 'localhost',
    'db': 'everydb',  # Database Name
    'user': 'root',
    'passwd': 'systemsss',
    'charset': 'utf8',
}

csv_exp_path = "C:\\Users\彰平\python_learn\KYS\csv\TenkaiHyoka_v1\\"
csv_exp_bk_path = "C:\\Users\彰平\python_learn\KYS\csv\TenkaiHyoka_v1\\bk\\"

# DBから指定日レース結果を取得 引数 年(yyyy)、開始月日(MMdd)、終了月日(MMdd)
def getTenkaiHyokaList(year, startDate, endDate):
    
    try:
        conn = MySQLdb.connect(host=db_config['host'], db=db_config['db'], user=db_config['user'],
                           passwd=db_config['passwd'], charset=db_config['charset'])    
        # カーソル取得
        #db = conn.cursor(buffered=True)
        db = conn.cursor(MySQLdb.cursors.DictCursor)

        # レース結果（ファイル名「検証_展開評価.sql」）
        filePath = "sql/tenkai_evaluation.sql"
        targetSqlFile = open(filePath)
        sql = targetSqlFile.read() # 終端まで読み込んだdataを返却
        #print(sql)
        db.execute(sql,[year,startDate,endDate])

        # 表示
        rows = db.fetchall()
        #print(rows)
        
        db.close()
        conn.close()
        
        return rows
    except:
        print("SQLファイル読み込みでエラー発生")

rows = getTenkaiHyokaList("2018", "1216", "1216")    
print(type(rows))
#print(rows)



# In[3]:

# 評価区分、文言作成
def makeHyokaMongon(resDict):
    
    # 条件(2018/12/20時点のルール) 
    #　先行有利と後方有利の判別方法を決める
    #  先行馬の定義・・・4角4番手以内
    #  後方馬の定義・・・4角5番手以降
    #
    # 先行有利の条件
    #  ①1人気ではない逃げ馬（4角1番手）が5着以内にいる
    #  ②先行馬が 5着以内に2頭いる
    # 後方有利の条件
    #  ③ 1人気ではない逃げ馬（4角1番手）が5着以内にいない
    #  ④後方馬が5着以内に3頭いる
    # 上記以外は標準
    
    # 評価区分　先行有利：1、標準：2、後方有利：3
    
    # 4角位置リスト（上位5頭）
    cornerList = [resDict["1Tyaku4C"],resDict["2Tyaku4C"],resDict["3Tyaku4C"],resDict["4Tyaku4C"],resDict["5Tyaku4C"]]
    #print(type(cornerList[1]))
    # 上位馬人気リスト（上位5頭）
    ninkiList = [resDict["1TyakuNinki"],resDict["2TyakuNinki"],resDict["3TyakuNinki"],resDict["4TyakuNinki"],resDict["5TyakuNinki"]]
    #print(ninkiList)
    
    hyokaMongon = ''
    hyokaKubun = 0

    # 4角先頭馬の人気、有無
    c4Ninki = 0
    # 4角1番手且1人気ではない馬がいる = True
    c4BanteFlg = False
    try:
        # 4角1番手の着順と人気
        cornerIdx = cornerList.index(1) 
        c4Ninki = ninkiList[cornerIdx]
        #print("4角1番手index:" + str(cornerIdx) + ",人気index:" + str(c4Ninki))
        
        # 4角1番手且1人気
        if c4Ninki != 1:
            c4BanteFlg = True
    except:
        #print("4角1番手 is none")
        pass
    
    # TODO ロジック再考          
    # 内包リスト
    senkouList = [i for i in cornerList if i <= 4 ]              
    kouhouList = [j for j in cornerList if j > 4 ]
              # 先行有利の判定
    # 4角1番手の有無で判別
    if c4BanteFlg:

        if len(senkouList) >= 2:
            #print('先行有利in①')
            hyokaMongon = '02SENK'
            hyokaKubun = 1
        else:
            #print('標準in④')
            hyokaMongon = '09STND'
            hyokaKubun = 2    
    else:
        if len(senkouList) >= 4:
            #print('先行有利in②')
            hyokaMongon = '02SENK'
            hyokaKubun = 1              
        elif len(kouhouList) >= 4:
            #print('後方有利in③')
            hyokaMongon = '0BKOHO'
            hyokaKubun = 3
            # todo 2018/12/26 ここまで
        else:
            #print('標準in④')
            hyokaMongon = '09STND'
            hyokaKubun = 2              

    
    # 辞書型で変換
    dic = {'HyokaKubun':hyokaKubun, 'HyokaMongon':hyokaMongon}
    return dic
    


# In[4]:

# 取得データの評価を行う（先行有利 or 後方有利 or 標準）
# 書き込み用データ（Target,csv両方に使用できるデータ群）用リストをreturn

# Target書き込み対象ファイル
# path = "C:\TFJV\MY_DATA\RMark2\"
# faleName =  RMyykp.DAT (yyは西暦下２桁、kは回次、pは場所名１字)
# ファイルフォーマット
#  行：日次とイコール
#　桁：評価文字列　→　1R6バイト「09標準」「02先行」「0B後方」 (09や0Bは背景カラー)

# csv書き込み用フォーマット（JV-Linkの主キー + 評価区分、評価文字列 ）

# 必要なアウトプット
# 西暦(4桁)、回次、場コード、場名、日次、R番号、血統番号、評価区分、評価文字列

def doEvaluationTenkai(raceResults):
    
    resultList = []
    
    
    for raceResult in raceResults:
        #print(raceResult)
        
        # アウトプットの整形
        seireki = raceResult['year']  # ここでは4桁
        kaiji = raceResult['Kaiji']
        jyoCd = raceResult['JyoCD']
        jyoName = raceResult['JyoName'] 
        nichiji = raceResult['Nichiji']
        raceNum = raceResult['RaceNum']
        #kettoNum = raceResult['KettoNum']
        #print(seireki + kaiji + nichiji + jyoName + raceNum)
        # 評価区分、文字列作成
        hyokaDic = ''
        try:
            hyokaDic = makeHyokaMongon(raceResult)
        except:
            print(seireki + kaiji + nichiji + jyoName + raceNum + " is 同着あり")
            pass

        if hyokaDic != '':
            hyokaKubun = hyokaDic['HyokaKubun'] 
            hyokaMongon = hyokaDic['HyokaMongon']
        else:
            hyokaKubun = '0'
            hyokaMongon = '      ' #半角スペース6つ
        
        #リストに設定
        resultHantei = [seireki, kaiji, jyoCd, jyoName, nichiji, raceNum, hyokaKubun, hyokaMongon]
        resultList.extend([resultHantei])  # pythonで2次元配列にするためには配列変数を[]で囲うとできる
        
    return resultList


# In[5]:

# レース番号ごとの置換する位置返却
def replaceIndexes(raceNum):
    
    # 返却リスト用の辞書 
    #  第一引数:左辺の終了文字数
    #  第二引数:右辺の開始インデックス
    replaceIndexesDic = {
        "01":[0,6],"02":[6,12],"03":[12,18],"04":[18,24],"05":[24,30],"06":[30,36],
        "07":[36,42],"08":[42,48],"09":[48,54],"10":[54,60],"11":[60,66],"12":[66,72]
    }

    return replaceIndexesDic.get(raceNum)
        


# In[6]:

# 行った評価をTargetFrontierの"R印2"に書き込み
def writeRaceMark2(resultHanteiList):
    
    # Target書き込み対象ファイル
    path = "C:\TFJV\MY_DATA\RMark2\\"
    
    
    lineIndex = 0
    # リスト分繰り返し
    for resultHantei in resultHanteiList:
        #print("loop start-------------------------")
        # ファイル名作成 faleName =  RMyykp.DAT (yyは西暦下２桁、kは回次、pは場所名１字)
        yy = resultHantei[0][2:]  # 年
        k = resultHantei[1][1:]   # 回次
        
        fileName = "RM" + yy + k + resultHantei[3] + ".DAT"
        #print("①" + fileName)
        filePath = path + fileName
        
        # 日次index
        nichijiIndex = int(resultHantei[4]) -1
        #print("②nichijiIndex = " + str(nichijiIndex))
        
        # ファイル存在確認 なければ作成（ベースファイルからコピー）
        if not os.path.isfile(filePath):
            with open(filePath, mode='w') as newF:
                newF.write('                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n')
    
        # 書き込み行が存在しない場合、ファイルへ行追加
        with open(filePath, 'r') as f3:
            lines = f3.readlines()
            #print("③" + str(lines))
            if len(lines) < nichijiIndex:
                print(fileName + "--------------行不足")
                with open(filePath, mode='a') as fa:
                    fa.write('                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n                                                                        \n')
                
        newLine = ''
        
        # ファイルオープン（なければ作成）
        with open(filePath,'r') as f1:
                    
            # ファイルフォーマット
            #  行index：日次 -1 とイコール
            #　桁：評価文字列　→　1R6バイト「09STND」「02SENK」「0BKOHO」 (09や0Bは背景カラー)
            # 行列移動
            # 評価文言挿入
            for i,inLine in enumerate(f1):
                #print("filelineIndex is " + str(i) + " , inLineStr = " + inLine)
                    
                
                # 更新対象行を特定
                # あれば、文言の更新
                if i == nichijiIndex:
                    
                    # raceNumによって置換する位置を特定
                    # 例
                    # s = 'abcdefghij'
                    # print(s[:4] + 'XXX' + s[7:])
                    # [結果] abcdXXXhij
                    
                    # レース番号による置換
                    repIdx = replaceIndexes(resultHantei[5])
                    #print("④" + str(repIdx))
                    
                    newLine = inLine[:repIdx[0]] + resultHantei[7] + inLine[repIdx[1]:]
                    #print("newLine = " + newLine)
                    break   
               
        
        # 更新用データの読み込み
        with open(filePath) as f2:
            l = f2.readlines()
            #print("⑤" + str(l))
        
        try:
            # 更新情報の挿入と、不要行の削除
            #print("⑥nichijiIndex: " + str(nichijiIndex))
            #print("⑦newLine = " + newLine)         
            l.insert(nichijiIndex, newLine)
            del l[nichijiIndex+1]
            #print("⑧" + str(l))
        except:
            print("①" + fileName)
            print("⑤" + str(l))
            print("⑥nichijiIndex: " + str(nichijiIndex))
            print("⑦newLine = " + newLine)

        # ファイルへの書き込み
        with open(filePath, 'w') as f3:
            f3.writelines(l)                    
                    
    # 繰り返し処理後、ファイルクローズ
    
    # 考慮　例外処理

    """ 【参考】　出遅れリスク処理のファイル読み書き処理
    # ファイル読み込み、ファイル内からレースＩＤを検索
    # with句はファイル閉じるを兼ねる
    with open(filePath,'r') as f1:
        
        for i,inLine in enumerate(f1):
        # 更新対象行を特定
        # あれば、文言の更新
            if raceId in inLine:
                #print(str(i) + ":" + inLine)
                lineIndex  = i
                newLine = inLine.rstrip('\n') + modelType + "\n"
                break
    
    # 更新用データの読み込み
    with open(filePath) as f2:
        l = f2.readlines()
    
    # 更新情報の挿入と、不要行の削除
    #print(lineIndex)
    #print(newLine)         
    l.insert(lineIndex, newLine)
    del l[lineIndex+1]
    
    # ファイルへの書き込み
    with open(filePath, 'w') as f3:
        f3.writelines(l)
    """

# 動確
#raceResults = getTenkaiHyokaList("2018", "1201", "1215")
#resultHanteiList = doEvaluationTenkai(raceResults)
#pprint.pprint(resultHanteiList)
#writeRaceMark2(resultHanteiList)    


# In[7]:

# csv出力


def exportTenkaiCsv(resultHanteiList, year, startDay, endDay):
    #print(type(resultHanteiList))
    df = pd.DataFrame(resultHanteiList, columns=['year','Kaiji','JyoCd','JyoName','Nichiji','RaceNum','HyokaKubun','HyokaMongon'])
    #print(df)
    
    csvFilePath = csv_exp_path + "TenkaiEva" + year + startDay + "-" + endDay + ".csv"
    # すでに存在するファイルはbkに移動
    if os.path.exists(csvFilePath):
        csvFileBkPath = glob.glob(csv_exp_bk_path + "TenkaiEva" + year + startDay + "-" + endDay + "*.csv")
        #print(csvFileBkPath)
        bkCnt = len(csvFileBkPath)
        if bkCnt > 0 :
            print("ファイル重複通過")
            # bkにはxxxx(1).csvって感じで入れる想定
            #f_bk = csv_exp_bk_path
            #bkCnt = f_bk.glob("TenkaiEva" + year + startDay + "-" + endDay + "*.csv")
            csv_exp_bk_newpath = csv_exp_bk_path + "TenkaiEva" + year + startDay + "-" + endDay + "(" + str(bkCnt) + ").csv"
            shutil.move(csvFilePath, csv_exp_bk_newpath)
        else:
            shutil.move(csvFilePath, csv_exp_bk_path)
    df.to_csv(path_or_buf=csvFilePath, encoding="shift-jis", index=False)
    


# In[8]:

# メイン処理


year = "2020"
startDay = "0128"
endDay = "0129"

startTime = datetime.now()
print("start:展開評価" + str(startTime))

# 対象Rの通過順位データの取得 --- A
raceResults = getTenkaiHyokaList(year, startDay, endDay)

# 取得データ分、評価を行う（先行有利 or 後方有利 or 標準）
resultHanteiList = doEvaluationTenkai(raceResults)
#pprint.pprint(resultHanteiList)

# 行った評価をTargetFrontierに書き込み
passTime1 = datetime.now()
print("FargetFrontier書き込み処理開始" + str(passTime1))
writeRaceMark2(resultHanteiList)
# データを出力
passTime2 = datetime.now()
print("csv書き込み処理開始" + str(passTime2))
exportTenkaiCsv(resultHanteiList,year,startDay,endDay)

endTime = datetime.now()
print("end:展開評価" + str(endTime))
print("処理時間" + str(endTime - startTime) ) 


# In[ ]:



