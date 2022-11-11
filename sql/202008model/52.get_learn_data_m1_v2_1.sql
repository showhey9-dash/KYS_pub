select 
 -- nur.Time,
 insert(insert(nur.Time,4,0,'.'),2,0,'.') as Time,
 -- nur.TimeDiff,
 -- nur.KakuteiJyuni,
 nur.Year, nur.MonthDay, nur.RaceNum, nur.Umaban,
 nur.JyoCD,
 nr.Kyori, nr.TrackCD, nr.SibaBabaCD, nr.DirtBabaCD, nr.JyokenCD1, nr.JyokenCD2, nr.JyokenCD3,
 -- nr.HaronTimeS3, nr.HaronTimeS4, nr.HaronTimeL4, nr.HaronTimeL3,
 -- nur.KettoNum, nur.Bamei, nur.Barei, nur.SexCD, nur.ZogenFugo, nur.ZogenSa,
 -- nur.HaronTimeL3,
 datediff(date(concat(nur.Year, nur.MonthDay)) , date(nus.BirthDate)) as afterBirth,
 nur.BaTaijyu,
 (nur.Futan / 10) as Futan,
 nus.F_syokin, nus.FType_syokin, nus.MF_syokin, nus.MFType_syokin,
 nus.MMF_syokin, nus.MMFType_syokin, nus.Breeder_syokin,
 ajs.Wakuban_val, ajs.Kisyu_val, ajs.Chokyosi_val, ajs.Father_val, ajs.FatherType_val,
 ajs.BMS_val, ajs.BMSType_val, ajs.Knicks_val
 
from n_uma_race nur
  left outer join n_race nr
    on nur.Year = nr.Year and nur.MonthDay = nr.MonthDay and nur.JyoCD = nr.JyoCD
    and nur.RaceNum = nr.RaceNum
  left outer join aggre_jadge_stock ajs
    on nur.Year = ajs.Year and nur.MonthDay = ajs.MonthDay and nur.JyoCD = ajs.JyoCD
    and nur.RaceNum = ajs.RaceNum and nur.Umaban = ajs.Umaban
  left outer join n_uma_spec_v2 nus
    on nur.KettoNum = nus.KettoNum
  
  where nur.Year >= 2015 and nur.Year <= 2021 and KakuteiJyuni > 0 and nur.JyoCD <= "10"
  and nus.F_syokin is not null and ajs.Wakuban_val is not null
  -- and nr.TrackCD like "1%" and nr.Kyori = 1600 and nr.SibaBabaCD = "1" 
  -- and nr.JyokenCD1 = "000" and nr.JyokenCD2 = "000" and nr.JyokenCD3 = "010"
order by nur.Time;  
