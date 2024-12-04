class Solution:
    def romanToInt(self, s: str) -> int:

        integer = 0
        pre_val = 4000
        for x in s:
            if x == "M":
                if pre_val == 100:
                    integer = integer + 800
                    pre_val = 1000
                else:
                    integer = integer + 1000
                    pre_val = 1000
            elif x == "D":
                if pre_val == 100:
                    integer = integer + 300
                    pre_val = 500
                else:
                    integer = integer + 500
                    pre_val = 500
            elif x == "C":
                if pre_val == 10:
                    integer = integer + 80
                    pre_val = 100
                else:
                    integer = integer + 100
                    pre_val = 100
            elif x == "L":
                if pre_val == 10:
                    integer = integer + 30
                    pre_val = 50
                else: 
                    integer = integer + 50
                    pre_val = 50
            elif x == "X":
                if pre_val == 1:
                    integer = integer + 8
                    pre_val = 10
                else:
                    integer = integer + 10
                    pre_val = 10
            elif x == "V":
                if pre_val == 1:
                    integer = integer + 3
                    pre_val = 5
                else:
                    integer = integer + 5
                    pre_val = 5
            elif x == "I":
                integer = integer + 1
                pre_val = 1
        return integer