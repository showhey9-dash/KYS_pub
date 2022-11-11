# pandasの列毎計算するサンプル
import pandas as pd
import numpy as np

df = pd.DataFrame(
    [[11, 1], [10, 1], [13, 0], [12, 1], [11, 0], [9, 0], [12, 1], [11, 0], [15, 0], [13, 0], [9, 1], [10, 0], [11, 0]],
    columns=['score', 'diagnosis'])

# 丸め誤差の影響を小さくするために整数でarange()して10で割る
X = np.arange(df['score'].min() * 10, df['score'].max() * 10 + 1) / 10
# print(X)
# numpy ndarrayで計算
score = df['score'].to_numpy()
# print(score)
# scoreを列ベクトルに変換してXと比較 → ブロードキャストで2次元配列になる
arr= (score[:, None] >= X).astype(np.int32)
# print(arr)
# それをデータフレーム化
df_x = pd.DataFrame(arr, columns=X)
print(df_x)
# 必要ならdfと結合
#df = pd.concat([df, df_x], axis=1)

# print(df)