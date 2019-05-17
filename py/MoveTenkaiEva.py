#!usr/bin/env python

import sys
import os

sys.path.append('C:\\Users\\彰平\\python_learn\\KYS\\py')
#sys.path

# 入力チェック
def inputChk(year, startDay, endDay):
    
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
    

# 自作のクラスを呼び出す
# クラス動確用
import Tenkai_Eva_Re2 as t2

def main():
    #引数をコマンドラインから入力
    year = input("Year?: ")
    startDay = input("StartDay?: ")
    endDay = input("EndDay?: ")

    if not inputChk(year, startDay, endDay):
        print('---end---')
        sys.exit()

    tenEva = t2.TenkaiEvaluation(year, startDay, endDay)

    #メイン処理を実行
    tenEva.execute()

if __name__ == '__main__':
    main()
    
        