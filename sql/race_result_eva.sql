-- 成績評価算出用
-- 前走１情報まで取得できる
-- jrdv_uma_raceを主にしてn_uma_raceをJOIN出来た
-- EXPLAIN;

select
    jur.jyoCd
  , jur.Year
  , jur.Kaiji
  , jur.Nichiji
  , nur.Nichiji as nur_nichiji
  , jur.RaceNum
  , jur.wakuban
  , jur.SyussoTosu
  , jur.RaceName
  , jur.Grade
  , jur.Jyoken
  , jur.Umaban
  , jur.Nengappi
  , jur.KettoNum
  , jur.Bamei
  , jur.KakuteiJyuni
  , jur.Ninki
  , (jur.Ninki - jur.KakuteiJyuni) EvaPoint
  , jur.Deokure
  , jur.Furi
  , jur.BabaCD
  , jur.Kyori
  , nur.Jyuni1c
  , nur.Jyuni2c
  , nur.Jyuni3c
  , nur.Jyuni4c
  , jur.RaceKyakusitsu
  , jur.DouchuUchiSoto
  , jur.Corner4Position
  , jur.GoalUchiSoto
  , jur.zen1seisekiKey
  , jur.zen1raceKey
  , jur.Bamei1 as WinBamei
  , jur.TimeDiff
  , jur.HaronTimeL3
  , jur.HaronTimeL3Jyuni
  , jur.HaronTimeL3Sa
  , jur.HaronTimeL3UchiSoto
-- 前走情報
  , jur2.jyoCd        as Zen1JyoCd
  , jur2.Year         as Zen1Year
  , jur2.Kaiji        as Zen1Kaiji
  , jur2.Nichiji      as Zen1Nichiji
  , jur2.RaceNum      as Zen1RaceNum
  , jur2.RaceName     as Zen1RaceName
  , jur2.Grade        as Zen1Grade
  , jur2.Jyoken       as Zen1Jyoken
  , jur2.KakuteiJyuni as Zen1KakuteiJyuni
  , jur2.Ninki        as Zen1Ninki
  , jur2.Kyori        as Zen1Kyori
  , jur2.TrackCD      as Zen1TrackCD
  , jur2.BabaCD       as Zen1BabaCD 
from
  jrdv_uma_race jur
  --  
  left outer join ( 
    select
        ur.jyoCd
      , SUBSTRING(ur.Year, 3) Year
      , SUBSTRING(ur.Kaiji, 2) Kaiji
      , SUBSTRING( 
        ( 
          case 
            when ur.Nichiji = 10 
              then '0a' 
            when ur.Nichiji = 11 
              then '0b' 
            when ur.Nichiji = 12 
              then '0c' 
            when ur.Nichiji = 13 
              then '0d' 
            when ur.Nichiji = 14 
              then '0e' 
            else ur.Nichiji 
            end
        ) 
        , 2
      ) Nichiji_16
      , ur.Nichiji
      , ur.RaceNum
      , ur.MonthDay
      , ur.Umaban
      , SUBSTRING(ur.KettoNum, 3) KettoNum
      , ur.Bamei
      , ur.Jyuni1c
      , ur.Jyuni2c
      , ur.Jyuni3c
      , ur.Jyuni4c 
    from
      n_uma_race ur 
    where
      ur.Year = %(year_new)s
      and ur.jyoCd = %(jyocd)s 
      and ur.RACENUM = %(racenum)s
  ) nur 
    on jur.jyoCd = nur.JyoCD 
    and jur.Year = nur.Year 
    and jur.Kaiji = nur.Kaiji
    and jur.Nichiji = nur.Nichiji_16
    and jur.RaceNum = nur.RaceNum 
    and jur.KettoNum = nur.KettoNum 
  left outer join ( 
    select
        jur2.jyoCd
      , jur2.Year
      , jur2.Kaiji
      , jur2.Nichiji
      , jur2.RaceNum
      , jur2.RaceName
      , jur2.Grade
      , jur2.Jyoken
      , jur2.Umaban
      , jur2.Bamei
      , jur2.KettoNum
      , jur2.KakuteiJyuni
      , jur2.Ninki
      , jur2.Kyori
      , jur2.TrackCD
      , jur2.BabaCD 
    from
      jrdv_uma_race jur2
  ) jur2 
    on SUBSTRING(jur.zen1raceKey, 1, 2) = jur2.jyoCd 
    and SUBSTRING(jur.zen1raceKey, 3, 2) = jur2.Year 
    and SUBSTRING(jur.zen1raceKey, 5, 1) = jur2.Kaiji 
    and SUBSTRING(jur.zen1raceKey, 6, 1) = jur2.Nichiji 
    and SUBSTRING(jur.zen1raceKey, 7) = jur2.RaceNum 
    and jur.KettoNum = jur2.KettoNum 
where
  jur.jyoCd = %(jyocd)s 
  and jur.Year = %(year)s
  and jur.Kaiji = %(kaiji)s
  and jur.Nichiji = %(nichiji)s 
  and jur.RACENUM = %(racenum)s 
order by
  jur.Umaban; 

-- 退避用
/*
select
    jur.jyoCd
  , jur.Year
  , jur.Kaiji
  , jur.Nichiji
  , jur.RaceNum
  , jur.wakuban
  , jur.SyussoTosu
  , jur.RaceName
  , jur.Grade
  , jur.Jyoken
  , jur.Umaban
  , jur.Nengappi
  , jur.KettoNum
  , jur.Bamei
  , jur2.Bamei
  , jur.KakuteiJyuni
  , jur.Ninki
  , (jur.Ninki - jur.KakuteiJyuni) EvaPoint
  , jur.Deokure
  , jur.Furi
  , jur.BabaCD
  , jur.Kyori
  , nur.Jyuni1c
  , nur.Jyuni2c
  , nur.Jyuni3c
  , nur.Jyuni4c
  , jur.DouchuUchiSoto
  , jur.Corner4Position
  , jur.GoalUchiSoto
  , jur.zen1seisekiKey
  , jur.zen1raceKey
  , 
-- 前走情報
  jur2.jyoCd          as Zen1JyoCd
  , jur2.Year         as Zen1Year
  , jur2.Kaiji        as Zen1Kaiji
  , jur2.Nichiji      as Zen1Nichiji
  , jur2.RaceNum      as Zen1RaceNum
  , jur2.RaceName     as Zen1RaceName
  , jur2.Grade        as Zen1Grade
  , jur2.Jyoken       as Zen1Jyoken
  , jur2.KakuteiJyuni as Zen1KakuteiJyuni
  , jur2.Ninki        as Zen1Ninki
  , jur2.Kyori        as Zen1Kyori
  , jur2.TrackCD      as Zen1TrackCD
  , jur2.BabaCD       as Zen1BabaCD 
from
  jrdv_uma_race jur
  --  
  left outer join ( 
    select
        ur.jyoCd
      , SUBSTRING(ur.Year, 3) Year
      , SUBSTRING(ur.Kaiji, 2) Kaiji
      , SUBSTRING( 
        ( 
          case 
            when ur.Nichiji = 10 
              then '0a' 
            when ur.Nichiji = 11 
              then '0b' 
            when ur.Nichiji = 12 
              then '0c' 
            when ur.Nichiji = 13 
              then '0d' 
            when ur.Nichiji = 14 
              then '0e' 
            else ur.Nichiji 
            end
        ) 
        , 2
      ) Nichiji
      , ur.RaceNum
      , ur.MonthDay
      , ur.Umaban
      , SUBSTRING(ur.KettoNum, 3) KettoNum
      , ur.Bamei
      , ur.Jyuni1c
      , ur.Jyuni2c
      , ur.Jyuni3c
      , ur.Jyuni4c 
    from
      n_uma_race ur 
    where
      ur.Year = '2019' 
      and ur.jyoCd = '06' 
      and ur.RACENUM = '11'
  ) nur 
    on jur.jyoCd = nur.JyoCD 
    and jur.Year = nur.Year 
    and jur.Kaiji = nur.Kaiji 
    and jur.Nichiji = nur.Nichiji 
    and jur.RaceNum = nur.RaceNum 
    and jur.KettoNum = nur.KettoNum 
  left outer join ( 
    select
        jur2.jyoCd
      , jur2.Year
      , jur2.Kaiji
      , jur2.Nichiji
      , jur2.RaceNum
      , jur2.RaceName
      , jur2.Grade
      , jur2.Jyoken
      , jur2.Umaban
      , jur2.Bamei
      , jur2.KettoNum
      , jur2.KakuteiJyuni
      , jur2.Ninki
      , jur2.Kyori
      , jur2.TrackCD
      , jur2.BabaCD 
    from
      jrdv_uma_race jur2
  ) jur2 
    on SUBSTRING(jur.zen1raceKey, 1, 2) = jur2.jyoCd 
    and SUBSTRING(jur.zen1raceKey, 3, 2) = jur2.Year 
    and SUBSTRING(jur.zen1raceKey, 5, 1) = jur2.Kaiji 
    and SUBSTRING(jur.zen1raceKey, 6, 1) = jur2.Nichiji 
    and SUBSTRING(jur.zen1raceKey, 7) = jur2.RaceNum 
    and jur.KettoNum = jur2.KettoNum 
where
  jur.jyoCd = '06' 
  and jur.Year = '19'
  and jur.Kaiji = '1'
  and jur.Nichiji = '1' 
  and jur.RACENUM = '11' 
order by
  jur.Umaban; 
*/
