-- aggre_summary_stock ast�W�v�p�A
-- ����������Factor��FactorValue��ǉ�
select * from aggre_summary_stock ast
where 
 ast.JyoCD = "01" and ast.TrackCD = "17" and ast.Kyori = "1200" and ast.BabaCode = "1"
 and ast.Factor = "�����t" and  ast.FactorValue Like "����%"

;
