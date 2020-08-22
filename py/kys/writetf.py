# Targetの予想コメント末尾に「　モデルＸ推奨馬」と記載
# ※※動作検証前

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
