-- ‘ÎÛƒŒ[ƒXŽæ“¾
select * from n_race r 
where r.year = %s  and (r.MonthDay >= %s and r.MonthDay <= %s)
 and  r.JyoCD <= '10' and r.TrackCD < '50'
 order by r.MonthDay, r.JyoCD, r.RaceNum ; 