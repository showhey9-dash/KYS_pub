-- 5�㌌���\�����擾
-- �N���X�f�[�^���Z�o���邽�߂̂��Ƃ�
-- �Q�l�f�[�^�@�T�[�g�D���i�[���A�i2016104505�j
-- select * from n_uma u where u.KettoNum = '2016104505';

select 
 u.MakeDate, u.KettoNum, u.BirthDate, u.Bamei, u.SexCD, u.KeiroCD,
 u.Ketto3InfoBamei1 as b1, 
 u.Ketto3InfoBamei2 as b2,
 u.Ketto3InfoBamei3 as b3, 
 u.Ketto3InfoBamei4 as b4,
 u.Ketto3InfoBamei5 as b5,
 u.Ketto3InfoBamei6 as b6,
 u.Ketto3InfoBamei7 as b7, 
 u.Ketto3InfoBamei8 as b8,
 u.Ketto3InfoBamei9 as b9, 
 u.Ketto3InfoBamei10 as b10,
 u.Ketto3InfoBamei11 as b11, 
 u.Ketto3InfoBamei12 as b12,
 u.Ketto3InfoBamei13 as b13, 
 u.Ketto3InfoBamei14 as b14,
 -- fff.HansyokuNum, fff.Bamei as fffBamei,
 -- �������̕���
 ffff.Bamei as b15,
 fffm.Bamei as b16,
 -- ������̕���
 ffmf.Bamei as b17,
 ffmm.Bamei as b18, 
 -- ���ꕃ�̕���
 fmff.Bamei as b19,
 fmfm.Bamei as b20,
 -- �����̕���
 fmmf.Bamei as b21,
 fmmm.Bamei as b22, 
 -- �ꕃ���̕���
 mfff.Bamei as b23,
 mffm.Bamei as b24, 
 -- �ꕃ��̕���
 mfmf.Bamei as b25,
 mfmm.Bamei as b26,  
 -- ��ꕃ�̕���
 mmff.Bamei as b27,
 mmfm.Bamei as b28,  
 -- ����̕���
 mmmf.Bamei as b29,
 mmmm.Bamei as b30,  

 -- ���������̕���
 fffff.Bamei as b31,
 ffffm.Bamei as b32,
  
 -- ��������̕���
 fffmf.Bamei as b33,
 fffmm.Bamei as b34,

 -- �����ꕃ�̕���
 ffmff.Bamei as b35,
 ffmfm.Bamei as b36,

 -- �������̕���
 ffmmf.Bamei as b37,
 ffmmm.Bamei as b38,

 -- ���ꕃ���̕���
 fmfff.Bamei as b39,
 fmffm.Bamei as b40,

 -- ���ꕃ��̕���
 fmfmf.Bamei as b41,
 fmfmm.Bamei as b42,

 -- ����ꕃ�̕���
 fmmff.Bamei as b43,
 fmmfm.Bamei as b44,

 -- ������̕���
 fmmmf.Bamei as b45,
 fmmmm.Bamei as b46,

 -- M
 -- �ꕃ�����̕���
 mffff.Bamei as b47,
 mfffm.Bamei as b48,
  
 -- �ꕃ����̕���
 mffmf.Bamei as b49,
 mffmm.Bamei as b50,

 -- �ꕃ�ꕃ�̕���
 mfmff.Bamei as b51,
 mfmfm.Bamei as b52,

 -- �ꕃ���̕���
 mfmmf.Bamei as b53,
 mfmmm.Bamei as b54,

 -- ��ꕃ���̕���
 mmfff.Bamei as b55,
 mmffm.Bamei as b56,

 -- ��ꕃ��̕���
 mmfmf.Bamei as b57,
 mmfmm.Bamei as b58,

 -- ���ꕃ�̕���
 mmmff.Bamei as b59,
 mmmfm.Bamei as b60,

 -- �����̕���
 mmmmf.Bamei as b61,
 mmmmm.Bamei as b62


 
 from n_uma u
 -- �������̕�����擾 
 left outer join n_hansyoku fff on u.Ketto3InfoHansyokuNum7 = fff.HansyokuNum
 left outer join n_hansyoku ffff on fff.HansyokuFNum = ffff.HansyokuNum
 left outer join n_hansyoku fffm on fff.HansyokuMNum = fffm.HansyokuNum
 -- ������̕�����擾
 left outer join n_hansyoku ffm on u.Ketto3InfoHansyokuNum8 = ffm.HansyokuNum
 left outer join n_hansyoku ffmf on ffm.HansyokuFNum = ffmf.HansyokuNum
 left outer join n_hansyoku ffmm on ffm.HansyokuMNum = ffmm.HansyokuNum
 -- ���ꕃ�̕�����擾
 left outer join n_hansyoku fmf on u.Ketto3InfoHansyokuNum9 = fmf.HansyokuNum
 left outer join n_hansyoku fmff on fmf.HansyokuFNum = fmff.HansyokuNum
 left outer join n_hansyoku fmfm on fmf.HansyokuMNum = fmfm.HansyokuNum
 -- �����̕�����擾
 left outer join n_hansyoku fmm on u.Ketto3InfoHansyokuNum10 = fmm.HansyokuNum
 left outer join n_hansyoku fmmf on fmm.HansyokuFNum = fmmf.HansyokuNum
 left outer join n_hansyoku fmmm on fmm.HansyokuMNum = fmmm.HansyokuNum
 -- �ꕃ���̕�����擾
 left outer join n_hansyoku mff on u.Ketto3InfoHansyokuNum11 = mff.HansyokuNum
 left outer join n_hansyoku mfff on mff.HansyokuFNum = mfff.HansyokuNum
 left outer join n_hansyoku mffm on mff.HansyokuMNum = mffm.HansyokuNum
 -- �ꕃ��̕�����擾
 left outer join n_hansyoku mfm on u.Ketto3InfoHansyokuNum12 = mfm.HansyokuNum
 left outer join n_hansyoku mfmf on mfm.HansyokuFNum = mfmf.HansyokuNum
 left outer join n_hansyoku mfmm on mfm.HansyokuMNum = mfmm.HansyokuNum
 -- ��ꕃ�̕�����擾
 left outer join n_hansyoku mmf on u.Ketto3InfoHansyokuNum13 = mmf.HansyokuNum
 left outer join n_hansyoku mmff on mmf.HansyokuFNum = mmff.HansyokuNum
 left outer join n_hansyoku mmfm on mmf.HansyokuMNum = mmfm.HansyokuNum
 -- ����̕�����擾
 left outer join n_hansyoku mmm on u.Ketto3InfoHansyokuNum14 = mmm.HansyokuNum
 left outer join n_hansyoku mmmf on mmm.HansyokuFNum = mmmf.HansyokuNum
 left outer join n_hansyoku mmmm on mmm.HansyokuMNum = mmmm.HansyokuNum
 
 -- ���������̕�����擾 
 left outer join n_hansyoku fffff on ffff.HansyokuFNum = fffff.HansyokuNum
 left outer join n_hansyoku ffffm on ffff.HansyokuMNum = ffffm.HansyokuNum
 
 -- ��������̕�����擾 
 left outer join n_hansyoku fffmf on fffm.HansyokuFNum = fffmf.HansyokuNum
 left outer join n_hansyoku fffmm on fffm.HansyokuMNum = fffmm.HansyokuNum

 -- �����ꕃ�̕�����擾 
 left outer join n_hansyoku ffmff on ffmf.HansyokuFNum = ffmff.HansyokuNum
 left outer join n_hansyoku ffmfm on ffmf.HansyokuMNum = ffmfm.HansyokuNum
 
 -- �������̕�����擾 
 left outer join n_hansyoku ffmmf on ffmm.HansyokuFNum = ffmmf.HansyokuNum
 left outer join n_hansyoku ffmmm on ffmm.HansyokuMNum = ffmmm.HansyokuNum

 -- ���ꕃ���̕�����擾 
 left outer join n_hansyoku fmfff on fmff.HansyokuFNum = fmfff.HansyokuNum
 left outer join n_hansyoku fmffm on fmff.HansyokuMNum = fmffm.HansyokuNum

 -- ���ꕃ��̕�����擾 
 left outer join n_hansyoku fmfmf on fmfm.HansyokuFNum = fmfmf.HansyokuNum
 left outer join n_hansyoku fmfmm on fmfm.HansyokuMNum = fmfmm.HansyokuNum

 -- ����ꕃ�̕�����擾 
 left outer join n_hansyoku fmmff on fmmf.HansyokuFNum = fmmff.HansyokuNum
 left outer join n_hansyoku fmmfm on fmmf.HansyokuMNum = fmmfm.HansyokuNum

 -- ������̕�����擾 
 left outer join n_hansyoku fmmmf on fmmm.HansyokuFNum = fmmmf.HansyokuNum
 left outer join n_hansyoku fmmmm on fmmm.HansyokuMNum = fmmmm.HansyokuNum


 -- M
 -- �ꕃ�����̕�����擾 
 left outer join n_hansyoku mffff on mfff.HansyokuFNum = mffff.HansyokuNum
 left outer join n_hansyoku mfffm on mfff.HansyokuMNum = mfffm.HansyokuNum
 
 -- �ꕃ����̕�����擾 
 left outer join n_hansyoku mffmf on mffm.HansyokuFNum = mffmf.HansyokuNum
 left outer join n_hansyoku mffmm on mffm.HansyokuMNum = mffmm.HansyokuNum

 -- �ꕃ�ꕃ�̕�����擾 
 left outer join n_hansyoku mfmff on mfmf.HansyokuFNum = mfmff.HansyokuNum
 left outer join n_hansyoku mfmfm on mfmf.HansyokuMNum = mfmfm.HansyokuNum
 
 -- �ꕃ���̕�����擾 
 left outer join n_hansyoku mfmmf on mfmm.HansyokuFNum = mfmmf.HansyokuNum
 left outer join n_hansyoku mfmmm on mfmm.HansyokuMNum = mfmmm.HansyokuNum

 -- ��ꕃ���̕�����擾 
 left outer join n_hansyoku mmfff on mmff.HansyokuFNum = mmfff.HansyokuNum
 left outer join n_hansyoku mmffm on mmff.HansyokuMNum = mmffm.HansyokuNum

 -- ��ꕃ��̕�����擾 
 left outer join n_hansyoku mmfmf on mmfm.HansyokuFNum = mmfmf.HansyokuNum
 left outer join n_hansyoku mmfmm on mmfm.HansyokuMNum = mmfmm.HansyokuNum

 -- ���ꕃ�̕�����擾 
 left outer join n_hansyoku mmmff on mmmf.HansyokuFNum = mmmff.HansyokuNum
 left outer join n_hansyoku mmmfm on mmmf.HansyokuMNum = mmmfm.HansyokuNum

 -- �����̕�����擾 
 left outer join n_hansyoku mmmmf on mmmm.HansyokuFNum = mmmmf.HansyokuNum
 left outer join n_hansyoku mmmmm on mmmm.HansyokuMNum = mmmmm.HansyokuNum

where
 u.BirthDate like %(year)s
 
 order by u.RuikeiHonsyoHeiti desc; 
 -- �N�G��10�b
 -- u.Bamei = '�T�[�g�D���i�[���A';
 

