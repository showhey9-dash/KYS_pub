-- JRDV系でレース結果と体系コードを取得

select
  case when jur_2.KakuteiJyuni <= '04' then 1 else 0 end as TaikeiTekisei,
  jur_2.jyoCd, jur_2.Kyori, jur_2.TrackCD, jur_2.LR, jur_2.BabaCD,
  jut.TaikeiCode
  from (
    select 
      jur.jyoCd, jur.Year, jur.Kaiji, jur.Nichiji, jur.RaceNum, jur.Umaban,
      jur.KettoNum, jur.Nengappi, jur.Bamei, jur.Kyori, jur.TrackCD, jur.LR,
      jur.courseInOut, jur.BabaCD, jur.Syubetu, jur.Jyoken, jur.RaceName, 
      jur.SyussoTosu, jur.KakuteiJyuni,jur.Time, jur.Futan, jur.KisyuName, 
      jur.ChokyosiName, jur.Ninki, jur.Deokure, jur.Furi, jur.TenkiCode,
      jur.RaceKyakusitsu
    from jrdv_uma_race jur 
      where (jur.Deokure != '' or jur.Furi != '') 
        and jur.KakuteiJyuni <= '04' 
        and (jur.Jyoken >= 08 or jur.Jyoken = 'OP') 
    union
    select
      jur.jyoCd, jur.Year, jur.Kaiji, jur.Nichiji, jur.RaceNum, jur.Umaban,
      jur.KettoNum, jur.Nengappi, jur.Bamei, jur.Kyori, jur.TrackCD, jur.LR,
      jur.courseInOut, jur.BabaCD, jur.Syubetu, jur.Jyoken, jur.RaceName, 
      jur.SyussoTosu, jur.KakuteiJyuni,jur.Time, jur.Futan, jur.KisyuName, 
      jur.ChokyosiName, jur.Ninki, jur.Deokure, jur.Furi, jur.TenkiCode,
      jur.RaceKyakusitsu    
    from jrdv_uma_race jur 
      where (jur.Deokure = '' and jur.Furi = '')
        and (jur.Jyoken >= 08 or jur.Jyoken = 'OP')
    ) jur_2 inner join jrdv_uma_taikei jut
    on jur_2.Year = jut.Year
      and jur_2.Kaiji = jut.Kaiji
      and jur_2.Nichiji = jut.Nichiji
      and jur_2.JyoCD = jut.jyoCd
      and jur_2.RaceNum = jut.RaceNum
      and jur_2.KettoNum = jut.KettoNum
  where
    jur_2.Nengappi >= '20170101' and jur_2.Nengappi < %(nengappi)s
  order by jur_2.Nengappi, jur_2.jyoCd, jur_2.RaceNum, jur_2.KakuteiJyuni
  ;