-- ��^�C�����Z�o����
-- 
-- ����
-- ���Y���[�X�̏�CD�A�����A�g���b�NCD�A�n��CD����v����ߋ�2�N����
-- 3���ȏ�1000���N���X�̕��Ϗ����^�C���ƃT���v����
-- �T���v���͍�_��2000m�ǁ@�i2021�N�H�؏܂Ɣ�r�j 
select 
  
  round(avg((r.LapTime1 + r.LapTime2 + r.LapTime3 + r.LapTime4 + r.LapTime5 + r.LapTime6 + r.LapTime7 + r.LapTime8 + r.LapTime9 + r.LapTime10 + r.LapTime11 + r.LapTime12 + r.LapTime13 + r.LapTime14 + r.LapTime15 + r.LapTime16 + r.LapTime17 + r.LapTime18) / 10), 1) as time
  , count(*) as kaisu
  -- *
 from n_race r 

where 
 r.JyoCD = %s and r.Kyori = %s and r.TrackCD like %s and (r.SibaBabaCD = %s and r.DirtBabaCD = %s)
 and r.JyokenCD3 = "010"
 and r.MakeDate > %s
 
group by r.JyoCD, r.Kyori, r.TrackCD ;
