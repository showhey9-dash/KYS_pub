select tmp1.year, tmp1.MonthDay, tmp1.JyoCD, tmp1.Kaiji, tmp1.Nichiji, case when tmp2.AtoTenkoCD is null then tmp1.AtoTenkoCD else tmp2.AtoTenkoCD end as AtoTenkoCD, case when tmp3.AtoSibaBabaCD is null then tmp1.AtoSibaBabaCD else tmp3.AtoSibaBabaCD end as AtoSibaBabaCD, case when tmp3.AtoDirtBabaCD is null then tmp1.AtoDirtBabaCD else tmp3.AtoDirtBabaCD end as AtoDirtBabaCD from (select * from s_tenko_baba b1 where b1.Year = %(year)s and b1.MonthDay = %(monthday)s and b1.HenkoID = "1") as tmp1 left outer join (select ttmp1.Year,ttmp1.MonthDay, ttmp1.JyoCD, ttmp1.Kaiji, ttmp1.Nichiji, ttmp1.HappyoTime, ttmp1.HenkoID, ttmp1.AtoTenkoCD, ttmp1.AtoSibaBabaCD, ttmp1.AtoDirtBabaCD from  ( select b1.Year,b1.MonthDay, b1.JyoCD, b1.Kaiji, b1.Nichiji, b1.HappyoTime, b1.HenkoID, b1.AtoTenkoCD, b1.AtoSibaBabaCD, b1.AtoDirtBabaCD from s_tenko_baba b1 where b1.HenkoID = "2" and b1.Year= %(year_2)s and b1.MonthDay = %(monthday_2)s ) ttmp1 join ( select b2.JyoCD, max(HappyoTime) as Happyo3 from s_tenko_baba b2 where b2.HenkoID = "2" and b2.Year= %(year_3)s and b2.MonthDay = %(monthday_3)s group by b2.JyoCD ) as b3 on ttmp1.JyoCD = b3.JyoCD and ttmp1.HappyoTime = b3.Happyo3 ) tmp2 on tmp1.JyoCD = tmp2.JyoCD left outer join (select  ttmp1.Year,ttmp1.MonthDay, ttmp1.JyoCD, ttmp1.Kaiji, ttmp1.Nichiji, ttmp1.HappyoTime, ttmp1.HenkoID, ttmp1.AtoTenkoCD, ttmp1.AtoSibaBabaCD, ttmp1.AtoDirtBabaCD from ( select b1.Year,b1.MonthDay, b1.JyoCD, b1.Kaiji, b1.Nichiji, b1.HappyoTime, b1.HenkoID, b1.AtoTenkoCD, b1.AtoSibaBabaCD, b1.AtoDirtBabaCD from s_tenko_baba b1 where b1.HenkoID = "3" and b1.Year= %(year_4)s and b1.MonthDay = %(monthday_4)s ) ttmp1 join ( select b2.JyoCD, max(HappyoTime) as Happyo3 from s_tenko_baba b2 where b2.HenkoID = "3" and b2.Year= %(year_5)s and b2.MonthDay = %(monthday_5)s group by b2.JyoCD ) as b3 on ttmp1.JyoCD = b3.JyoCD and ttmp1.HappyoTime = b3.Happyo3) tmp3 on tmp1.JyoCD = tmp3.JyoCD;
