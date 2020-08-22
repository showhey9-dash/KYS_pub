-- Aggre_summary_stockからの集計用
-- Yearはサマリーする
-- 抽出対象6件以上

select 

 9999, -- 年毎に集計していない場合は9999とする
 ast.JyoCD, ast.TrackCD, ast.Kyori, ast.BabaCode, ast.Factor, ast.FactorValue,
 sum(tyaku1) as tyaku1_2,
 sum(tyaku2) as tyaku2_2,
 sum(tyaku3) as tyaku3_2,
 sum(kei) as kei_2,
 round(sum(tyaku1) / sum(kei) * 100, 1) as win_2,
 round((sum(tyaku1) + sum(tyaku2)) / sum(kei) * 100, 1) as rentai_rate_2,
 round((sum(tyaku1) + sum(tyaku2) + sum(tyaku3)) / sum(kei) * 100, 1) as hukusyo_rate_2
 
 
from Aggre_summary_stock ast

where ast.JyoCD = %(jyocd)s and ast.TrackCD = %(trackcd)s and ast.Kyori = %(kyori)s and ast.BabaCode = %(babacd)s

group by ast.JyoCD, ast.TrackCD,ast.Kyori, ast.BabaCode, ast.Factor, ast.FactorValue
having sum(kei) > 5 and hukusyo_rate_2 >= 25
order by hukusyo_rate_2 desc
-- limit 500
;
