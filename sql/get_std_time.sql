-- 基準タイムを算出する
-- 
-- 条件
-- 当該レースの場CD、距離、トラックCD、馬場CDが一致する過去2年分の
-- 3差以上1000万クラスの平均勝ちタイムとサンプル数
-- サンプルは阪神芝2000m良　（2021年秋華賞と比較） 
select 
  
  round(avg((r.LapTime1 + r.LapTime2 + r.LapTime3 + r.LapTime4 + r.LapTime5 + r.LapTime6 + r.LapTime7 + r.LapTime8 + r.LapTime9 + r.LapTime10 + r.LapTime11 + r.LapTime12 + r.LapTime13 + r.LapTime14 + r.LapTime15 + r.LapTime16 + r.LapTime17 + r.LapTime18) / 10), 1) as time
  , count(*) as kaisu
  -- *
 from n_race r 

where 
 r.JyoCD = %s and r.Kyori = %s and r.TrackCD like %s and (r.SibaBabaCD = %s and r.DirtBabaCD = %s)
 and r.JyokenCD3 = "010"
 and r.MakeDate > %s
 
group by r.JyoCD, r.Kyori, r.TrackCD ;
