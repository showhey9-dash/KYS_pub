#!usr/bin/env python
# クラス作成勉強用
class Spam:
    val = 100
    def ham(self):
        self.egg('call method')

    def egg(self,msg):
        print("{0}".format(msg))
        print(("{0}".format(self.val)))

#spam = Spam()
#spam.ham()