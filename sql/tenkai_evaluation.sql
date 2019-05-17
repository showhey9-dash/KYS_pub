-- ���[�X�W�J�]���pSQL�쐬 2018/12/19
-- �������5����4�p���ʂƐl�C��1�s�ɒ��o�������B�R�[�X����n��R�[�h���~������ 

select 
  ur.year, ur.MonthDay, ur.JyoCD
  , cm.CODE_NAME2 as JyoName  -- �ꖼ
  , ur.Kaiji, ur.Nichiji, ur.RaceNum
  , sum(case when ur.KakuteiJyuni = '01' then ur.Jyuni4c end) 1Tyaku4C
  , sum(case when ur.KakuteiJyuni = '02' then ur.Jyuni4c end) 2Tyaku4C
  , sum(case when ur.KakuteiJyuni = '03' then ur.Jyuni4c end) 3Tyaku4C
  , sum(case when ur.KakuteiJyuni = '04' then ur.Jyuni4c end) 4Tyaku4C
  , sum(case when ur.KakuteiJyuni = '05' then ur.Jyuni4c end) 5Tyaku4C
  , sum(case when ur.KakuteiJyuni = '01' then ur.Ninki end) 1TyakuNinki
  , sum(case when ur.KakuteiJyuni = '02' then ur.Ninki end) 2TyakuNinki
  , sum(case when ur.KakuteiJyuni = '03' then ur.Ninki end) 3TyakuNinki
  , sum(case when ur.KakuteiJyuni = '04' then ur.Ninki end) 4TyakuNinki
  , sum(case when ur.KakuteiJyuni = '05' then ur.Ninki end) 5TyakuNinki
  , r.JyokenCD5, r.Kyori, r.TrackCD, r.TenkoCD, r.SibaBabaCD
  , case when r.SibaBabaCD = 0 then r.DirtBabaCD else r.SibaBabaCD end BabaCd
  , r.HaronTimeS3 / 10 as HaronTimeS3
  , r.HaronTimeS4 / 10 as HaronTimeS4
  , r.HaronTimeL4 / 10 as HaronTimeL4
  , r.HaronTimeL3 / 10 as HaronTimeL3
  , (r.HaronTimeL4 - r.HaronTimeL3)  / 10 as L4fRap
    
from n_uma_race ur left outer join n_race r
  on ur.Year = r.Year and ur.MonthDay = r.MonthDay and ur.JyoCD = r.JyoCD 
    and ur.Kaiji = r.Kaiji and ur.Nichiji = r.Nichiji and ur.RaceNum = r.RaceNum
  left outer join code_master cm
    on ur.JyoCD = cm.CODE_VALUE
  where ur.year = %s  and (ur.MonthDay >= %s and ur.MonthDay <= %s)
    and  ur.JyoCD <= '10' and r.TrackCD < '50'and cm.CODE_ID = '2001'
  group by ur.year, ur.JyoCD, ur.Kaiji, ur.Nichiji, ur.RaceNum
  order by ur.year, ur.MonthDay, ur.JyoCD, ur.RaceNum;