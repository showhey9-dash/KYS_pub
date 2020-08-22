-- 体系適正 試験データ取得用SQL
-- 該当日に出走する馬たちの体系コードとレース情報

select
  jut.jyoCd, 
  jrd.Kyori,
  jrd.TrackCD,
  jrd.LR,
  jut.TaikeiCode,
  jut.Year, jut.Kaiji, jut.Nichiji, jut.RaceNum, jut.Umaban, jut.Bamei
  
  from jrdv_uma_taikei jut inner join jrdv_race_data jrd 
    on jut.jyoCd = jrd.jyoCd 
    and jut.Year = jrd.Year
    and jut.Kaiji = jrd.Kaiji
    and jut.Nichiji = jrd.Nichiji
    and jut.RaceNum = jrd.RaceNum
   where jut.Nengappi = %(nengappi)s
   order by jut.Nengappi desc, jut.jyoCd, jut.RaceNum, jut.Umaban;