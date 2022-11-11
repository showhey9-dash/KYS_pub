-- 当日のレースに出走する馬情報を取得
select
 ur.*,
 mcs1.SYUBOBA_NAME as Father,
 mcst1.TYPE_NAME as FatherType,
 mcs5.SYUBOBA_NAME as Bms,
 mcst5.TYPE_NAME as BmsType
 from n_uma_race ur
  left outer join n_uma u on ur.KettoNum = u.KettoNum
  left outer join m_check_syuboba mcs1 on u.Ketto3InfoBamei1 = mcs1.SYUBOBA_NAME
  left outer join m_check_syuboba_type mcst1 on mcs1.TYPE_CODE = mcst1.TYPE_CODE
  left outer join m_check_syuboba mcs5 on u.Ketto3InfoBamei5 = mcs5.SYUBOBA_NAME
  left outer join m_check_syuboba_type mcst5 on mcs5.TYPE_CODE = mcst5.TYPE_CODE

 where ur.Year = %(year)s and ur.MonthDay = %(monthday)s and
 ur.JyoCD = %(jyocd)s and ur.RaceNum = %(racenum)s
 and ur.Umaban > '00' and ur.DataKubun = '7' order by umaban;
