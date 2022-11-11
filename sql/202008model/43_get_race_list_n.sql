select
 r.Year, r.MonthDay, r.JyoCD, r.Kaiji, r.Nichiji, r.RaceNum, r.TrackCD, r.Kyori, r.SibaBabaCD, r.DirtBabaCD 
 from n_race r where Year = %(year)s and r.MonthDay >= %(startDay)s and r.MonthDay <= %(endDay)s and r.JyoCD <= "10" ; 