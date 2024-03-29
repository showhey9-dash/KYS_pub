-- 当日のレースに出走する馬情報を取得
select
 -- ur.*,
 -- us.*,
 -- ajs.*
 ur.Year, ur.MonthDay, ur.JyoCD, ur.Kaiji, ur.Nichiji, ur.RaceNum, ur.Umaban,
 ur.KettoNum, ur.Bamei, 
 us.F_syokin, us.FType_syokin, us.MF_syokin, us.MFType_syokin, 
 us.MMF_syokin, us.MMFType_syokin, us.Breeder_syokin,
 ajs.Wakuban_val, ajs.Kisyu_val, ajs.Chokyosi_val, ajs.Father_val,
 ajs.FatherType_val, ajs.BMS_val, ajs.BMSType_val, ajs.Knicks_val
 from s_uma_race ur
  left outer join n_uma u on ur.KettoNum = u.KettoNum
  left outer join n_uma_spec us on ur.KettoNum = us.KettoNum
  left outer join aggre_jadge_stock ajs
    on ur.Year = ajs.Year and ur.MonthDay = ajs.MonthDay and ur.JyoCD = ajs.JyoCD and ur.RaceNum = ajs.RaceNum and ur.Umaban = ajs.Umaban

 where ur.Year = %(year)s and ur.MonthDay = %(monthday)s and
 ur.JyoCD = %(jyocd)s and ur.RaceNum = %(racenum)s
 and ur.Umaban > '00' order by ur.umaban; 
