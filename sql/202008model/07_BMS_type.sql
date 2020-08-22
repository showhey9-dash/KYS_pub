-- ��R�[�h�A�g���b�N�R�[�h�A�����A�n���Ԗ���
-- BMS�^�C�v���̋������ʂ��擾����
-- �W�v�J�n���� 2015/01/01�A�����n���Ώ�

insert into Aggre_summary_stock

select
 ur.Year,
 ur.JyoCD, r.TrackCD, r.Kyori, 
-- r.DirtBabaCD,
 case 
   when r.TrackCD <= 22 then r.SibaBabaCD
   else r.DirtBabaCD
 end as BabaCode,
 "BMS�^�C�v", -- �t�@�N�^�[
 vcs.TYPE_NAME,
-- vcs.SYUBOBA_NAME,
-- u.Ketto3InfoBamei5,
 count(ur.KakuteiJyuni=1 or Null) as 1tyaku,
 count(ur.KakuteiJyuni=2 or Null) as 2tyaku,
 count(ur.KakuteiJyuni=3 or Null) as 3tyaku,
 count(*) as kei,
 round(count(ur.KakuteiJyuni=1 or Null) / count(*) * 100, 1) as win,
 round((count(ur.KakuteiJyuni=1 or Null) + count(ur.KakuteiJyuni=2 or Null)) / count(*) * 100, 1) as rentai_rate,
 round((count(ur.KakuteiJyuni=1 or Null) + count(ur.KakuteiJyuni=2 or Null) + count(ur.KakuteiJyuni=3 or Null)) / count(*) * 100, 1) as hukusyo_rate,
 CURDATE() 
 
from n_uma_race ur left outer join n_race r
 on ur.Year = r.Year and ur.MonthDay = r.MonthDay and ur.JyoCD = r.JyoCD
 and ur.Kaiji = r.Kaiji and ur.Nichiji = r.Nichiji and ur.RaceNum = r.RaceNum
 left outer join n_uma u 
 on ur.KettoNum = u.KettoNum
 left outer join v_check_shuboba vcs
 on u.Ketto3InfoBamei5 = vcs.SYUBOBA_NAME

where
 r.DataKubun = "7" and
 ur.Year >= %(cond_year)s and ur.KakuteiJyuni > 0 and
 ur.JyoCD = %(jyocd)s and
 r.Kyori = %(kyori)s and
 r.TrackCD = %(trackcd)s
 and vcs.TYPE_NAME is not null  -- ���Ԃ�����NG
group by  ur.Year, ur.JyoCD, r.TrackCD, r.Kyori, r.SibaBabaCD, r.DirtBabaCD, vcs.TYPE_NAME

order by kei desc
 
;


