-- 払戻データ join 馬レース join レース
-- 条件：場コード、年、回次、日次、レース番号、馬レース.馬番
select
  h.JyoCD,
  h.Year,
  h.Kaiji,
  h.Nichiji,
  h.RaceNum,
  ur.Umaban,
  -- コース、条件
  r.JyokenCD5 as Jyoken,
  r.Kyori,
  r.TrackCD,
  r.SibaBabaCD,
  r.DirtBabaCD,
  -- 競争結果
  ur.KakuteiJyuni,
  ur.Ninki,

  h.PayTansyoUmaban1,
  h.PayTansyoPay1,
  h.PayTansyoNinki1,
  h.PayTansyoUmaban2,
  h.PayTansyoPay2,
  h.PayTansyoNinki2,
  h.PayFukusyoUmaban1,
  h.PayFukusyoPay1,
  h.PayFukusyoNinki1,
  h.PayFukusyoUmaban2,
  h.PayFukusyoPay2,
  h.PayFukusyoNinki2,
  h.PayFukusyoUmaban3,
  h.PayFukusyoPay3,
  h.PayFukusyoNinki3,
  h.PayFukusyoUmaban4,
  h.PayFukusyoPay4,
  h.PayFukusyoNinki4,
  h.PayFukusyoUmaban5,
  h.PayFukusyoPay5,
  h.PayFukusyoNinki5

 from n_harai h left outer join n_uma_race ur
   on h.Year = ur.Year
   and h.JyoCD = ur.JyoCD
   and h.Kaiji = ur.Kaiji
   and h.Nichiji = ur.Nichiji
   and h.RaceNum = ur.RaceNum
   left outer join n_race r
     on  h.Year = r.Year
     and h.JyoCD = r.JyoCD
     and h.Kaiji = r.Kaiji
     and h.Nichiji = r.Nichiji
     and h.RaceNum = r.RaceNum
  where h.year = %(year)s
    and h.JyoCD = %(jyocd)s
    and h.Kaiji = %(kaiji)s
    and h.Nichiji = %(nichiji)s
    and h.RaceNum = %(racenum)s
    and ur.Umaban = %(umaban)s
 ;