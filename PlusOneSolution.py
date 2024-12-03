class Solution:
    def plusOne(self, digits: List[int]) -> List[int]:
        x = len(digits) - 1
        digits[x] =  digits[x] + 1
        i = len(digits) - 2
        if digits == [10]:
            digits = [1,0]
        elif digits[x] == 10:
            digits[x] = 0
            while i > -1:
                print(digits)
                if digits[i] != 9:
                    digits[i] = digits[i] + 1
                    break
                elif i == 0:
                    digits[i] = 1
                    digits.append(0)
                    break
                elif digits[i] == 9:
                    digits[i] = 0
                    i = i - 1
        return digits