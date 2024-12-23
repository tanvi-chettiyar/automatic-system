class Solution:
    def isPalindrome(self, x: int) -> bool:
        stringInput = str(x)
        reverseInput = stringInput[::-1]
        if stringInput == reverseInput:
            return True
        else:
            return False